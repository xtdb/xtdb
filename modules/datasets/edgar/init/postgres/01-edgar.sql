-- EDGAR fundamentals sec-master schema + CDC publication for the XTDB Postgres source.
--
-- This file is the source of truth for the Postgres schema. The loader
-- (xtdb.datasets.edgar.pg) only UPSERTs — it does not create tables — so the
-- tables and publication all live here and exist on first boot.
--
-- Postgres holds current state only: each statement row UPSERTs by its period
-- key, keeping the most recently filed vintage (latest-`filed`-wins). The
-- restatement history and the instant valid-time timeline live in XTDB.
--
-- The line-item columns are the snake-cased XBRL concepts the registry loads
-- (xtdb.datasets.edgar.parse/statement-column-names); keep them in lockstep.
-- All money/count line items are `numeric` so they stay exact.

-- static issuer reference, one row per filer.
CREATE TABLE issuer (
    cik         text PRIMARY KEY,
    entity_name text,
    filed       date
);

-- income_statement — duration (flows). Keyed by the full period (start, end):
-- a duration figure is fixed for the window it reports; a restatement re-files
-- the same period under a new accession.
CREATE TABLE income_statement (
    cik                                                       text,
    period_start                                              date,
    period_end                                                date,
    gross_profit                                              numeric,
    net_cash_provided_by_used_in_operating_activities         numeric,
    net_income_loss                                           numeric,
    operating_income_loss                                     numeric,
    research_and_development_expense                          numeric,
    revenue_from_contract_with_customer_excluding_assessed_tax numeric,
    revenues                                                  numeric,
    accession                                                 text,
    form                                                      text,
    filed                                                     date,
    PRIMARY KEY (cik, period_start, period_end)
);

-- balance_sheet — instant (balances). Keyed by the as-of date (end): an instant
-- balance supersedes the prior as-of value.
CREATE TABLE balance_sheet (
    cik                                                                  text,
    period_end                                                           date,
    assets                                                               numeric,
    assets_current                                                       numeric,
    cash_cash_equivalents_restricted_cash_and_restricted_cash_equivalents numeric,
    common_stock_shares_issued                                           numeric,
    common_stock_shares_outstanding                                      numeric,
    entity_common_stock_shares_outstanding                               numeric,
    goodwill                                                             numeric,
    liabilities                                                          numeric,
    liabilities_current                                                  numeric,
    stockholders_equity                                                  numeric,
    accession                                                            text,
    form                                                                 text,
    filed                                                                date,
    PRIMARY KEY (cik, period_end)
);

-- DELETEs carry only the replica-identity columns; the default (primary key)
-- identity is what EdgarPgIndexer needs to derive _id on a delete.
-- FOR ALL TABLES covers the tables above (and anything added later) — the EDGAR
-- stack has its own database, so there's nothing else here to over-publish.
CREATE PUBLICATION edgar_pub FOR ALL TABLES;
