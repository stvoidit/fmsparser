package fmsparser

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

var (
	counter uint64
)

// эту ерунду можно удалить, просто для наглядности статистики.
// принтить постоянно кол-во - очень накладная операция,
// лучше выводить по таймеру счетчики
func printCounter() {
	var prevCounter uint64
	var prevTime time.Time
	start := time.Now()
	prevTime = start
	t := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-t.C:
			countSince := counter - prevCounter
			fmt.Println(
				"elapsed time:", time.Since(start), "\t",
				"read lines:", counter, "\t",
				"speed:", countSince, time.Since(prevTime))
			prevCounter = counter
			prevTime = time.Now()
		}
	}
}

// ParserFMS - ...
type ParserFMS struct {
	wg     *sync.WaitGroup    // стандартная waitgroup для горутин
	wrch   chan CSVrow        // канал куда кидаем строчки из csv, откуда и читаем
	db     *Store             // соединение с бд, пул соединений
	err    error              // передача ошибки как элемента основной структуры, сюда пишем и жмем cancel
	cancel context.CancelFunc // рычаг стоп, если вылетит ошибка
	ctx    context.Context    // общий контекс, где будет ожидаться <-ctx.Done() если вылетит ошибка
}

// NewParser - количество воркеров нежелательно ставить больше,
// чем кол-во рабочих процессов postgres, либо больше чем х2 их кол-ва,
// т.к. не будет иметь смысла.
// При вставке более массивных объектов, это может сильно затормозить его,
// т.к. если повесить еще и апдейты (на что-то другое), то в параллель будет
// вставать автовакуум и прочее, из-за чего добавление в БД замедлиться.
func NewParser(workers, packsize int, db *Store) *ParserFMS {
	if workers <= 0 {
		workers = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	var p = ParserFMS{
		wrch:   make(chan CSVrow),
		wg:     new(sync.WaitGroup),
		db:     db,
		ctx:    ctx,
		cancel: cancel}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			p.WriteWorker(packsize)
		}()
	}
	return &p
}

// InsertData - построчное чтение, более сейвово по памяти
func (p *ParserFMS) InsertData(r io.Reader) error {
	go printCounter()
	start := time.Now()
	defer p.db.Close()
	defer p.wg.Wait()
	defer close(p.wrch)
	if r == nil {
		return errors.New("Reader is nil")
	}
	s := bufio.NewScanner(r)
loop:
	for s.Scan() {
		// пропускаем шапку
		if counter == 0 {
			counter++
			continue
		}
		select {
		case <-p.ctx.Done():
			break loop
		default:
			row := strings.Split(s.Text(), ",")
			p.wrch <- CSVrow{Series: row[0], Number: row[1]}
			counter++
		}
	}
	fmt.Println(time.Since(start))
	return p.err
}

// WriteWorker - Если при инициализации экземпляра ParserFMS
// был задам параметр packsize <= 0, то паки записей будут скидывать в конце.
// По идее, единственная разница между сбросом в дефолте - объем потребления оперативки на хранение
func (p *ParserFMS) WriteWorker(packsize int) {
	defer p.wg.Done()
	var pack = make([]CSVrow, 0)
	for row := range p.wrch {
		pack = append(pack, row)
		// по умолчанию, чтобы не стоять и не ждать, закидываем в БД паки
		// если для размера пака установлено значение 0 или меньше,
		// то финальный пак каждого воркера полетит в конце
		if packsize >= 1 && len(pack) == packsize {
			if err := p.db.InsertorUpdate(pack...); err != nil && p.err == nil {
				p.err = err
				p.cancel()
			}
			pack = pack[:0]
		}
	}
	// докидываем остатки в паке
	if err := p.db.InsertorUpdate(pack...); err != nil && p.err == nil {
		p.err = err
		p.cancel()
	}
}
