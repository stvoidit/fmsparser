-- Drop table
-- DROP TABLE public.passports;
CREATE TABLE public.passports (
    id serial NOT NULL,
    passport_series varchar(5) NOT NULL,
    passport_number varchar(10) NOT NULL,
    created timestamp NOT NULL DEFAULT now(),
    CONSTRAINT passports_un UNIQUE (passport_series, passport_number)
);