-- GLEIF sec-master schema + CDC publication for the XTDB Postgres source.
--
-- This file is the source of truth for the Postgres schema. The loader
-- (xtdb.datasets.gleif.pg) only UPSERTs — it does not create tables — so the
-- tables, enum types and publication all live here and exist on first boot.
--
-- Native enum types hold the kebab values the mirror emits, so PG and XT agree;
-- an out-of-enum value fails the insert (surfacing unmodelled data). The `lei`
-- table is current state per entity; each LegalEntityEventType has its own table.
--
-- Relationships (the rr file) are deferred — no lei_relationship table yet.

-- enum types (kebab of the LEI-CDF 3.1 XSD enum values)
CREATE TYPE entity_status AS ENUM ('active', 'inactive', 'null');
CREATE TYPE registration_status AS ENUM (
    'pending-validation', 'issued', 'duplicate', 'lapsed', 'merged', 'retired',
    'annulled', 'cancelled', 'transferred', 'pending-transfer', 'pending-archival');
CREATE TYPE entity_category AS ENUM (
    'general', 'branch', 'fund', 'sole-proprietor',
    'resident-government-entity', 'international-organization');
CREATE TYPE event_status AS ENUM ('in-progress', 'withdrawn-cancelled', 'completed');
CREATE TYPE event_group_type AS ENUM (
    'reverse-takeover', 'standalone', 'change-legal-form-and-name', 'complex-change-legal-form');

-- current state, one row per entity. nested bits (addresses, other-names) as jsonb.
CREATE TABLE lei (
    lei                        text PRIMARY KEY,
    legal_name                 text,
    legal_address              jsonb,
    hq_address                 jsonb,
    jurisdiction               text,
    legal_form_code            text,
    entity_category            entity_category,
    entity_status              entity_status,
    entity_creation_date       timestamptz,
    registration_status        registration_status,
    initial_registration_date  timestamptz,
    last_update_date           timestamptz,
    next_renewal_date          timestamptz,
    managing_lou               text,
    other_names                jsonb
);

-- one table per LegalEntityEventTypeEnum value (the authoritative 19), same shape:
-- the 5 change-* attributes as lei_<attr>_changes, the 14 occurrences pluralised.
-- (lei, effective_date) PK collapses the verbatim re-sends GLEIF emits per publish.
--
-- The five change-* tables are *state transitions*, not occurrences: a GLEIF
-- change event carries only a date, never the new value — the value lives on the
-- snapshot. So we bake the then-current value (attribute-named column) into the
-- change row at load time, making each a self-contained "the value became X,
-- effective Y". A snapshot only ever shows the one change that produced its
-- current value, so the pairing is 1:1. Mirrored into XT keyed by lei with
-- _valid_from = effective_date, these become real bitemporal state history.
CREATE TABLE lei_legal_name_changes                  (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, legal_name text,        PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_other_name_changes                  (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, other_names jsonb,      PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_legal_address_changes               (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, legal_address jsonb,    PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_hq_address_changes                  (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, hq_address jsonb,       PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_legal_form_changes                  (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, legal_form_code text,   PRIMARY KEY (lei, effective_date));

-- the remaining fourteen are *occurrences* — things that happened at a point and
-- don't supersede a prior value (an entity can have several). Date-only; no value
-- to carry. Mirrored into XT keyed by lei__effective_date.
CREATE TABLE lei_demergers                           (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_spinoffs                            (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_absorptions                         (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_acquisition_branches                (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_transformation_branch_to_subsidiary (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_transformation_subsidiary_to_branch (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_transformation_umbrella_to_standalone (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_breakups                            (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_mergers_and_acquisitions            (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_bankruptcies                        (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_liquidations                        (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_voluntary_arrangements              (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_insolvencies                        (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));
CREATE TABLE lei_dissolutions                        (lei text, effective_date timestamptz, recorded_date timestamptz, status event_status, group_type event_group_type, validation_documents text, PRIMARY KEY (lei, effective_date));

-- DELETEs carry only the replica-identity columns; the default (primary key)
-- identity is what GleifPgIndexer needs to derive _id on a delete.
-- FOR ALL TABLES covers the tables above (and anything added later).
CREATE PUBLICATION gleif_pub FOR ALL TABLES;
