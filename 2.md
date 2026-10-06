# Загрузка HH

Сначала прочитайте общие правила: [README.md](README.md).

Ваша схема `vacancies_app` в общей БД **не создаётся** — данные идут в общие таблицы. Отличия от вашего проекта:

| Ваш проект | Общая схема |
|---|---|
| `vacancies` (39 колонок) | ядро `vacancy` + `vacancy_hh` |
| `employer_profiles` | `employer_profile` (`source = 'hh'`) |
| `companies` | не нужна: юрлица — существующая таблица `company` (ГУР) |
| `employer_company_matches`, `vacancy_company_matches` | `employer_company_match`, `vacancy_company_match` — те же статусы и правила проверки, добавлены `auto` и `rejected` |
| `vacancy_routes` | `vacancy_discovery` |
| `html_snapshots` | `vacancy_snapshot` (`format = 'html'`) |
| поля фильтра в карточке | прогон классификации `hh_filter` |

## Источники данных

- `data/hh_fast/full_cards.jsonl` — все 20 075 полных карточек (включая review/excluded).
- `queue.sqlite3` (checkpoint) — все маршруты поиска и метаданные HTML.
- `index.jsonl` — **не загружать**: короткие записи без полной карточки.

## employer_profile

Создавать только при непустых `employer_source_id` **и** `employer_id_namespace`. По схожести названий профили не склеивать.

| Колонка | Значение |
|---|---|
| `source` | `'hh'` |
| `id_namespace` | `employer_id_namespace` |
| `external_id` | `employer_source_id` |
| `name` | `employer_name_detail`, а если пусто — `employer_name` |
| `url` | `employer_profile_url` (сейчас пусто → `NULL`) |
| `inn`, `ogrn`, `kpp` | `NULL` (у HH нет) |
| `publisher_type` | `'unknown'` — агентство только при подтверждении источником, не по слову «персонал» в названии |
| `first_seen_at` / `last_seen_at` | min / max `observed_at` по вакансиям профиля |

Ожидается 4 718 профилей. 40 карточек без ID профиля → `vacancy.employer_profile_id = NULL`.

## vacancy

| Ваша колонка | Колонка `vacancy` | Примечание |
|---|---|---|
| `source` = `'hh_html'` | `source` = `'hh'` | будущий HH API — тоже `'hh'`, ключ один |
| `source_vacancy_id` | `external_id` | |
| `source_url` | `url` | |
| — | `employer_profile_id` | FK из шага выше |
| `employer_name` | `employer_name` | имя как в карточке, не заменять названием завода |
| `title` | `title` | |
| `region` | **`locality`** | у вас это город; `region_id` (субъект РФ) оставить `NULL` |
| `address` | `address` | |
| `salary_from`, `salary_to`, `salary_currency`, `salary_gross`, `salary_period` | одноимённые | `salary_gross=false` и `0` — не `NULL`; `unspecified_by_source` — как есть |
| `experience` | `experience` | `experience_years` — `NULL` |
| `education` | `education` | |
| `schedule` | `schedule` | |
| `description` | `description` | |
| `responsibilities` | `responsibilities` | |
| `requirements` | `requirements` | |
| `conditions` | `conditions` | |
| `skills` | `skills_raw` | **как строка**, не делить по запятым |
| `published_at_source` | `published_at` | |
| — | `modified_at` | `NULL` |
| `observed_at` | `first_seen_at`, `last_seen_at` | |
| — | `is_active` | `true` |
| — | `profession`, `profession_code`, `specialisation`, `lat`, `lng` | `NULL` |

## vacancy_hh

| Ваша колонка | Колонка |
|---|---|
| `employer_name_detail` | `employer_name_detail` |
| `employer_resolution_status` | `employer_resolution_status` |
| `address_type` | `address_type` |
| `detail_status` | `detail_status` |
| `source_visibility` | `source_visibility` |

Не переносятся:

- `detail_url` — если он отличается от `source_url`, напишите: добавим колонку.
- `employer_source_id`, `employer_id_namespace`, `employer_profile_url` уже в `employer_profile`.
- `public_business_contact_name`, `public_business_phone`, `public_business_email` не храним, см. общие правила.
- `filter_decision`, `filter_matches`, `discovery_route`, `vpk_status`, `vpk_evidence` загружаются ниже.

## vacancy_discovery

Главный `discovery_route` из карточки плюс все маршруты из `routes` в checkpoint SQLite — по строке на `(vacancy_id, route)`. `first_seen_at` / `last_seen_at` — только если они есть в SQLite, иначе `NULL`.

## vacancy_snapshot

По строке на каждый сохранённый HTML: `format = 'html'`, `path` — путь к HTML.gz относительно корня хранилища (включая старые из `data/hh_bulk/raw`), `fetched_at`, `http_status`, `sha256`, `locator = NULL`. Сообщите владельцу БД, где будет лежать корень хранилища.

## Классификация: прогон hh_filter

Одна строка `classifier_run`:

```sql
INSERT INTO classifier_run (classifier, version, params, started_at, note)
VALUES ('hh_filter', '<версия фильтра или хэш конфига>',
        '{"level_map": {"candidate_seed_employer": "...", "candidate_strong_signal": "...",
                        "<review-значение>": "review", "<excluded-значение>": "no"}}',
        '<время работы фильтра>', 'перенос полей filter_* из карточек')
RETURNING run_id;
```

Заполните `level_map` на всех фактических значениях `filter_decision`. Предлагаемое соответствие такое:
- `candidate_seed_employer` → `likely`: работодатель из стартового списка, но сама вакансия не проверялась;
- `candidate_strong_signal` → `likely`;
- review-группа → `review`;
- excluded → `no`.

Если считаете, что seed-работодатель уже даёт `confirmed`, укажите это в `note`.

Для каждой карточки:

| Колонка | Значение |
|---|---|
| `vacancy_classification.level` | по `level_map` |
| `raw_label` | `filter_decision` без изменений |
| `score` | `NULL`, если фильтр не даёт оценку |
| `category` | `vpk_status`, если это категория; иначе `NULL` и `vpk_status` в признаки |
| `classification_evidence` | по строке на совпадение из `filter_matches`, если его можно надёжно разделить. `signal` — что совпало, `origin` — `own_text` / `employer_text` / `company_name`, `snippet` — фрагмент. Если разделить нельзя — одна строка `signal = 'filter_matches'`, `snippet` = вся строка; `vpk_evidence` — отдельной строкой `signal = 'vpk_evidence'` |

Это результат фильтра, а не подтверждение ВПК — так и остаётся.

## Сопоставление с предприятиями

Автоматическое сопоставление по ИНН для HH невозможно, ИНН нет. Ваши будущие NLP-гипотезы «этот профиль — такое-то юрлицо» или «эту вакансию фактически даёт завод X» пишите так:

- `employer_company_match` или `vacancy_company_match`;
- `status = 'predicted'`, `method = 'nlp'`, `model_version`, `confidence`, `evidence` (JSON с аргументами);
- `company_id` — из `company` (найти по `inn`, `name_full_ru`, `name_short_ru`).

## Проверка

Кроме общих запросов из [README.md](README.md):

```sql
SELECT count(*) FROM vacancy WHERE source = 'hh';                          -- 20 075
SELECT count(*) FROM employer_profile WHERE source = 'hh';                 -- 4 718
SELECT count(*) FROM vacancy WHERE source = 'hh' AND employer_profile_id IS NULL;  -- 40
SELECT count(*) FROM vacancy v JOIN vacancy_hh USING (vacancy_id) WHERE v.source = 'hh';  -- 20 075
SELECT raw_label, level, count(*) FROM vacancy_classification c
JOIN classifier_run r USING (run_id) WHERE r.classifier = 'hh_filter' GROUP BY 1, 2;
```
