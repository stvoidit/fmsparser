package fmsparser

import (
	"github.com/jackc/pgx/v4"
)

// CSVrow - строка со значениями из csv
type CSVrow struct {
	Series, Number string
}

// InsertorUpdate - добавление или обновление
func (s *Store) InsertorUpdate(rows ...CSVrow) error {
	// нужна проверка, иначе после result.Exec будет падение из-за вставки пустоты
	if len(rows) == 0 {
		return nil
	}
	cur, ctx, err := s.Cursor()
	if err != nil {
		return err
	}
	defer cur.Release() // релиз отпускает соединение обратно в пул
	// выполнение внури транзакции позволяет не блокировать внутренний insert постгреса.
	// Тут 2 стула:
	// - блокируемся об внутренний wait insert постгреса
	// или
	// - блокируемся об wal writer и прочие штуки для атомарности у постгреса
	// В любом из случаев - батчить через прямой коннект или транзакцию - БД самое узкое горлышко
	tx, err := cur.Begin(ctx)
	if err != nil {
		return err
	}
	// вариант вставки через EXISTS более правильный, но долгий
	// вариант через игнорирование ошибки - легче, в логи не гадит, результат тот же
	const insert = `
	INSERT INTO passports
		(passport_series, passport_number)
	SELECT $1::varchar, $2::varchar
		-- WHERE NOT EXISTS (SELECT 1 FROM passports WHERE passport_series = $1::varchar AND passport_number = $2::varchar)
		ON CONFLICT (passport_series, passport_number) DO NOTHING`
	var b = new(pgx.Batch)
	for _, row := range rows {
		b.Queue(insert, row.Series, row.Number)
	}
	result := tx.SendBatch(ctx, b)
	if _, err := result.Exec(); err != nil {
		return err
	}
	if err := result.Close(); err != nil {
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}
