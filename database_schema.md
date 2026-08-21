# Database Schema

## Table: `public.app_users`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('app_users_id_seq'::regclass)` |
| 2 | `username` | `text` | NO | `` |
| 3 | `password_hash` | `text` | NO | `` |
| 4 | `role` | `text` | NO | `'user'::text` |
| 5 | `active` | `boolean` | NO | `true` |
| 6 | `created_at` | `timestamp with time zone` | NO | `now()` |
| 7 | `updated_at` | `timestamp with time zone` | NO | `now()` |

## Table: `public.customers`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `integer` | NO | `nextval('customers_id_seq'::regclass)` |
| 2 | `customer_name` | `text` | YES | `` |
| 3 | `full_address` | `text` | YES | `` |
| 4 | `contact_person` | `text` | YES | `` |
| 5 | `phone_number` | `text` | YES | `` |
| 6 | `notes` | `text` | YES | `` |

## Table: `public.jobs_cache`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('jobs_cache_id_seq'::regclass)` |
| 2 | `job_code` | `text` | NO | `` |
| 3 | `job_name` | `text` | NO | `` |
| 4 | `customer` | `text` | YES | `` |
| 5 | `active` | `integer` | NO | `1` |
| 6 | `source_updated_at` | `text` | YES | `` |
| 7 | `refreshed_at` | `text` | YES | `` |
| 8 | `tax_exempt` | `text` | NO | `''::text` |

## Table: `public.manual_jobs`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('manual_jobs_id_seq'::regclass)` |
| 2 | `job_code` | `text` | NO | `` |
| 3 | `job_name` | `text` | NO | `` |
| 4 | `customer` | `text` | NO | `''::text` |
| 5 | `tax_exempt` | `text` | NO | `''::text` |
| 6 | `active` | `integer` | NO | `1` |
| 7 | `created_at` | `text` | NO | `` |

## Table: `public.material_predictions`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('material_predictions_id_seq'::regclass)` |
| 2 | `ticket_number` | `text` | NO | `` |
| 3 | `ticket_created_at` | `text` | NO | `''::text` |
| 4 | `material_entry` | `text` | NO | `''::text` |
| 5 | `image_url` | `text` | NO | `''::text` |
| 6 | `predicted_label` | `text` | NO | `''::text` |
| 7 | `top1_confidence` | `real` | YES | `` |
| 8 | `top_k_json` | `text` | NO | `''::text` |
| 9 | `raw_output_json` | `text` | NO | `''::text` |
| 10 | `model_name` | `text` | NO | `''::text` |
| 11 | `model_path` | `text` | NO | `''::text` |
| 12 | `inference_ms` | `integer` | YES | `` |
| 13 | `status` | `text` | NO | `''::text` |
| 14 | `error_message` | `text` | NO | `''::text` |
| 15 | `created_at` | `timestamp with time zone` | NO | `now()` |
| 16 | `updated_at` | `timestamp with time zone` | NO | `now()` |
| 17 | `ticket_id` | `bigint` | YES | `` |
| 18 | `predicted_at` | `timestamp with time zone` | NO | `now()` |

## Table: `public.material_prediction_feedback`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('material_prediction_feedback_id_seq'::regclass)` |
| 2 | `prediction_id` | `bigint` | NO | `` |
| 3 | `ticket_id` | `bigint` | NO | `` |
| 4 | `ticket_material` | `text` | NO | `` |
| 5 | `predicted_material` | `text` | NO | `` |
| 6 | `feedback_type` | `text` | NO | `` |
| 7 | `correct_material` | `text` | YES | `` |
| 8 | `feedback_given_at` | `timestamp with time zone` | NO | `now()` |
| 9 | `feedback_given_by` | `text` | NO | `''::text` |

## Table: `public.material_price`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `integer` | NO | `nextval('material_price_id_seq'::regclass)` |
| 2 | `cat` | `integer` | NO | `` |
| 3 | `material` | `text` | NO | `` |
| 4 | `direction` | `text` | NO | `` |
| 5 | `axle_1` | `double precision` | YES | `` |
| 6 | `tandem` | `double precision` | YES | `` |
| 7 | `triaxle` | `double precision` | YES | `` |
| 8 | `axle_4_5` | `double precision` | YES | `` |
| 10 | `axle_6` | `double precision` | YES | `` |
| 11 | `semi` | `double precision` | YES | `` |
| 12 | `hydvac` | `double precision` | YES | `` |
| 13 | `dirt_in` | `double precision` | YES | `` |
| 14 | `active` | `boolean` | NO | `true` |
| 15 | `cost_per_cy` | `double precision` | NO | `0` |

## Table: `public.materials`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('materials_id_seq'::regclass)` |
| 2 | `material_name` | `text` | NO | `` |
| 3 | `active` | `integer` | NO | `1` |

## Table: `public.rfid_epc_truck_map`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('rfid_epc_truck_map_id_seq'::regclass)` |
| 2 | `epc` | `text` | NO | `` |
| 3 | `truck_number` | `text` | NO | `` |
| 4 | `source` | `text` | NO | `''::text` |
| 5 | `active` | `boolean` | NO | `true` |
| 6 | `created_at` | `timestamp with time zone` | NO | `now()` |
| 7 | `updated_at` | `timestamp with time zone` | NO | `now()` |

## Table: `public.rfid_notifications`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('rfid_notifications_id_seq'::regclass)` |
| 2 | `event_type` | `text` | NO | `'known_truck_detected'::text` |
| 3 | `truck_number` | `text` | NO | `` |
| 4 | `source` | `text` | NO | `'rfid'::text` |
| 5 | `message` | `text` | NO | `` |
| 6 | `detected_at` | `timestamp with time zone` | YES | `` |
| 7 | `created_at` | `timestamp with time zone` | NO | `now()` |
| 8 | `status` | `text` | NO | `'pending'::text` |
| 9 | `decided_at` | `timestamp with time zone` | YES | `` |

## Table: `public.rfid_tag_catalog`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `serial_number` | `text` | NO | `` |
| 2 | `epc` | `text` | NO | `` |
| 3 | `active` | `boolean` | NO | `true` |
| 4 | `created_at` | `timestamp with time zone` | NO | `now()` |
| 5 | `updated_at` | `timestamp with time zone` | NO | `now()` |

## Table: `public.rfid_truck_tag_assignment`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('rfid_truck_tag_assignment_id_seq'::regclass)` |
| 2 | `serial_number` | `text` | NO | `` |
| 3 | `truck_id` | `bigint` | NO | `` |
| 4 | `notes` | `text` | NO | `''::text` |
| 5 | `assigned_at` | `timestamp with time zone` | NO | `now()` |
| 6 | `updated_at` | `timestamp with time zone` | NO | `now()` |

## Table: `public.ticket_audit_log`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('ticket_audit_log_id_seq'::regclass)` |
| 2 | `ticket_id` | `bigint` | NO | `` |
| 3 | `ticket_number` | `text` | NO | `''::text` |
| 4 | `action` | `text` | NO | `` |
| 5 | `actor_user_id` | `bigint` | YES | `` |
| 6 | `actor_username_snapshot` | `text` | NO | `''::text` |
| 7 | `before_data` | `jsonb` | NO | `'{}'::jsonb` |
| 8 | `after_data` | `jsonb` | NO | `'{}'::jsonb` |
| 9 | `changed_fields` | `jsonb` | NO | `'[]'::jsonb` |
| 10 | `note` | `text` | NO | `''::text` |
| 11 | `created_at` | `timestamp with time zone` | NO | `now()` |

## Table: `public.ticket_sequence`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `ticket_year` | `integer` | NO | `` |
| 2 | `last_value` | `integer` | NO | `` |

## Table: `public.tickets`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('tickets_id_seq'::regclass)` |
| 2 | `ticket_number` | `text` | NO | `` |
| 3 | `ticket_year` | `integer` | NO | `` |
| 4 | `ticket_sequence` | `integer` | NO | `` |
| 5 | `direction` | `text` | NO | `` |
| 6 | `created_at` | `text` | NO | `` |
| 7 | `job_id` | `integer` | YES | `` |
| 8 | `job_code_snapshot` | `text` | NO | `` |
| 9 | `job_name_snapshot` | `text` | NO | `` |
| 10 | `customer_snapshot` | `text` | NO | `''::text` |
| 11 | `truck_id` | `integer` | YES | `` |
| 12 | `truck_number_snapshot` | `text` | NO | `` |
| 13 | `material_id` | `integer` | YES | `` |
| 14 | `material_name_snapshot` | `text` | NO | `` |
| 15 | `quantity` | `real` | NO | `` |
| 16 | `unit` | `text` | NO | `` |
| 17 | `notes` | `text` | YES | `` |
| 18 | `pdf_path` | `text` | YES | `` |
| 19 | `pdf_blob` | `bytea` | YES | `` |
| 20 | `tax_exempt` | `text` | NO | `''::text` |
| 21 | `cost` | `real` | NO | `0` |
| 22 | `active` | `boolean` | NO | `true` |
| 23 | `modified_to_id` | `bigint` | YES | `` |
| 24 | `image_url` | `text` | NO | `''::text` |
| 25 | `image_status` | `text` | NO | `''::text` |
| 26 | `image_error` | `text` | NO | `''::text` |
| 27 | `image_generated_at` | `text` | YES | `` |
| 28 | `created_by_user_id` | `bigint` | YES | `` |
| 29 | `updated_by_user_id` | `bigint` | YES | `` |
| 30 | `deleted_by_user_id` | `bigint` | YES | `` |
| 31 | `updated_at` | `timestamp with time zone` | NO | `now()` |
| 32 | `deleted_at` | `timestamp with time zone` | YES | `` |
| 33 | `voided_at` | `timestamp with time zone` | YES | `` |
| 34 | `created_by` | `text` | NO | `''::text` |
| 35 | `updated_by` | `text` | YES | `` |

## Table: `public.trucks`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('trucks_id_seq'::regclass)` |
| 2 | `truck_number` | `text` | NO | `` |
| 3 | `description` | `text` | YES | `` |
| 4 | `truck_size` | `text` | NO | `''::text` |
| 5 | `hauled_by` | `text` | NO | `''::text` |
| 6 | `active` | `integer` | NO | `1` |

## Table: `public.trucks_main`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `integer` | NO | `nextval('trucks_main_id_seq'::regclass)` |
| 2 | `truck_number` | `text` | NO | `` |
| 3 | `trucking_company` | `text` | NO | `''::text` |
| 4 | `notes` | `text` | YES | `` |
| 5 | `truck_size` | `text` | NO | `''::text` |
| 6 | `phone` | `text` | YES | `` |
| 7 | `license_plate` | `text` | YES | `` |
| 8 | `active` | `boolean` | NO | `true` |

