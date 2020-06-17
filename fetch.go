package fmsparser

import (
	"bytes"
	"compress/bzip2"
	"io"
	"net/http"
)

var (
	defaultHeaders = http.Header{
		`Accept`:                    {`text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9`},
		`Accept-Encoding`:           {`gzip, deflate, sdch`},
		`Accept-Language`:           {`ru,en;q=0.9,ja;q=0.8`},
		`Cache-Control`:             {`no-cache`},
		`Connection`:                {`keep-alive`},
		`DNT`:                       {`1`},
		`Host`:                      {`guvm.mvd.ru`},
		`Pragma`:                    {`no-cache`},
		`Upgrade-Insecure-Requests`: {`1`},
		`User-Agent`:                {`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 YaBrowser/20.6.1.148 Yowser/2.5 Safari/537.36`},
	}
)

// FetchArchive - просто скачиваем архив и отдаем ридер после декомпреса
func (p *ParserFMS) FetchArchive() (io.Reader, error) {
	const link = `http://guvm.mvd.ru/upload/expired-passports/list_of_expired_passports.csv.bz2`
	c := http.DefaultClient
	req, err := http.NewRequest(`GET`, link, nil)
	if err != nil {
		return nil, err
	}
	req.Header = defaultHeaders.Clone()
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	return bzip2.NewReader(&buf), nil
}

// FetchAndInsert - скачать и сразу начать загрузку
func (p *ParserFMS) FetchAndInsert() error {
	r, err := p.FetchArchive()
	if err != nil {
		return err
	}
	return p.InsertData(r)
}
