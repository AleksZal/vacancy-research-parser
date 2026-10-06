-- PostgreSQL application database. Does not modify collector queue.sqlite3.
-- Run once against a separate application database, not the collector checkpoint.
BEGIN;
CREATE SCHEMA vacancies_app;
SET LOCAL search_path = vacancies_app, pg_catalog;

-- Publisher profiles are identities on a job site, not verified legal entities.
CREATE TABLE employer_profiles (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_platform text NOT NULL,
    id_namespace text NOT NULL,
    source_employer_id text NOT NULL,
    display_name text NOT NULL,
    profile_url text,
    publisher_type text NOT NULL DEFAULT 'unknown'
        CHECK (publisher_type IN ('unknown', 'company', 'agency', 'recruitment_center', 'other')),
    publisher_type_evidence jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(publisher_type_evidence) = 'array'),
    UNIQUE (source_platform, id_namespace, source_employer_id),
    CHECK (btrim(source_employer_id) <> ''),
    CHECK (btrim(id_namespace) <> ''),
    UNIQUE (id, source_platform)
);

-- All 39 original export fields remain here, with their original names.
-- The record represents the latest collected full card, not its whole history.
CREATE TABLE vacancies (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_platform text NOT NULL,
    employer_profile_id bigint,

    source text NOT NULL,
    source_vacancy_id text NOT NULL,
    source_url text NOT NULL,
    detail_url text,
    observed_at timestamptz NOT NULL,
    published_at_source timestamptz,
    detail_status text NOT NULL,
    source_visibility text,
    title text NOT NULL,
    employer_name text,
    employer_name_detail text,
    employer_source_id text,
    employer_id_namespace text,
    employer_profile_url text,
    employer_resolution_status text,
    region text,
    address text,
    address_type text,
    salary_from numeric(18,2),
    salary_to numeric(18,2),
    salary_currency text,
    salary_gross boolean,
    salary_period text,
    experience text,
    description text,
    responsibilities text,
    requirements text,
    conditions text,
    education text,
    skills text,
    schedule text,
    public_business_contact_name text,
    public_business_phone text,
    public_business_email text,
    filter_decision text,
    filter_matches text,
    discovery_route text,
    vpk_status text NOT NULL DEFAULT 'unverified',
    vpk_evidence text,

    UNIQUE (source_platform, source_vacancy_id),
    FOREIGN KEY (employer_profile_id, source_platform)
        REFERENCES employer_profiles (id, source_platform),
    CHECK (btrim(source_vacancy_id) <> ''),
    CHECK (salary_from IS NULL OR salary_from >= 0),
    CHECK (salary_to IS NULL OR salary_to >= 0)
);
-- Original employer_* fields preserve the names/identity seen on this card.
-- employer_profiles contains the shared current profile; do not overwrite the
-- card's original employer names with a guessed legal entity or client name.

CREATE INDEX vacancies_employer_idx ON vacancies (employer_profile_id);
CREATE INDEX vacancies_filter_idx ON vacancies (filter_decision);
CREATE INDEX vacancies_region_idx ON vacancies (region);
CREATE INDEX vacancies_published_idx ON vacancies (published_at_source DESC);

-- A vacancy may be discovered by many search phrases/employer searches.
CREATE TABLE vacancy_routes (
    vacancy_id bigint NOT NULL REFERENCES vacancies (id),
    route text NOT NULL CHECK (btrim(route) <> ''),
    first_seen_at timestamptz,
    last_seen_at timestamptz,
    PRIMARY KEY (vacancy_id, route),
    CHECK (last_seen_at >= first_seen_at)
);

-- Keep existing gzip HTML files; store references and integrity information.
-- Old baseline snapshots may physically reside under data/hh_bulk/raw.
CREATE TABLE html_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vacancy_id bigint NOT NULL REFERENCES vacancies (id),
    request_url text NOT NULL,
    observed_at timestamptz NOT NULL,
    http_status smallint NOT NULL CHECK (http_status BETWEEN 100 AND 599),
    storage_path text NOT NULL,
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    content_encoding text NOT NULL DEFAULT 'gzip',
    UNIQUE (vacancy_id, observed_at, content_sha256)
);

-- Legal entities from GUR / other registries, independent of HH profiles.
CREATE TABLE companies (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    legal_name text NOT NULL,
    country_code text NOT NULL DEFAULT 'RU',
    inn text,
    ogrn text,
    gur_entity_id text,
    gur_profile_url text,
    official_website text,
    vpk_status text NOT NULL DEFAULT 'unverified',
    evidence jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(evidence) = 'array'),
    latitude numeric(9,6) CHECK (latitude BETWEEN -90 AND 90),
    longitude numeric(9,6) CHECK (longitude BETWEEN -180 AND 180),
    location_evidence jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(location_evidence) = 'array'),
    UNIQUE (country_code, inn),
    CHECK ((latitude IS NULL) = (longitude IS NULL)),
    CHECK (inn IS NULL OR btrim(inn) <> '')
);
-- NULL INN values can repeat. Name equality is not a uniqueness criterion.

CREATE TABLE employer_company_matches (
    employer_profile_id bigint NOT NULL REFERENCES employer_profiles (id),
    company_id bigint NOT NULL REFERENCES companies (id),
    status text NOT NULL DEFAULT 'candidate'
        CHECK (status IN ('candidate', 'verified', 'rejected')),
    confidence numeric(5,4) CHECK (confidence BETWEEN 0 AND 1),
    method text NOT NULL,
    evidence jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(evidence) = 'array'),
    reviewed_by text,
    reviewed_at timestamptz,
    PRIMARY KEY (employer_profile_id, company_id),
    CHECK (status <> 'verified' OR
        (jsonb_array_length(evidence) > 0 AND reviewed_by IS NOT NULL AND reviewed_at IS NOT NULL))
);
CREATE INDEX employer_company_matches_company_idx ON employer_company_matches (company_id, status);

-- Actual hiring company inferred from a particular vacancy (e.g. agency client).
-- Kept separate from the publisher's own legal-entity identity.
CREATE TABLE vacancy_company_matches (
    vacancy_id bigint NOT NULL REFERENCES vacancies (id),
    company_id bigint NOT NULL REFERENCES companies (id),
    status text NOT NULL DEFAULT 'predicted'
        CHECK (status IN ('predicted', 'verified', 'rejected')),
    confidence numeric(5,4) CHECK (confidence BETWEEN 0 AND 1),
    method text NOT NULL,
    model_version text,
    evidence jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(evidence) = 'array'),
    reviewed_by text,
    reviewed_at timestamptz,
    PRIMARY KEY (vacancy_id, company_id),
    CHECK (status <> 'verified' OR
        (jsonb_array_length(evidence) > 0 AND reviewed_by IS NOT NULL AND reviewed_at IS NOT NULL))
);
CREATE INDEX vacancy_company_matches_company_idx ON vacancy_company_matches (company_id, status);

-- A flat view with exactly the same 39 fields and order as the current exports.
CREATE VIEW vacancy_export AS
SELECT source, source_vacancy_id, source_url, detail_url, observed_at,
       published_at_source, detail_status, source_visibility, title,
       employer_name, employer_name_detail, employer_source_id,
       employer_id_namespace, employer_profile_url, employer_resolution_status,
       region, address, address_type, salary_from, salary_to, salary_currency,
       salary_gross, salary_period, experience, description, responsibilities,
       requirements, conditions, education, skills, schedule,
       public_business_contact_name, public_business_phone, public_business_email,
       filter_decision, filter_matches, discovery_route, vpk_status, vpk_evidence
FROM vacancies;
COMMIT;
