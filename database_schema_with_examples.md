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

### Example Data

| id | username | password_hash | role | active | created_at | updated_at |
| --- | --- | --- | --- | --- | --- | --- |
| 4 | Mike | scrypt:32768:8:1$wM6fY6wvrpr969Vh$c23534f71f913a51549ca1432be94dfd2cf2b738e3e08a454900d2bbfffd97e6a73b3cce364ce85f62523026f98befd6f401b4b0e82a158d42349e953ddeff48 | admin | True | 2026-07-15 15:14:14.680976+00:00 | 2026-07-15 15:14:45.836232+00:00 |
| 6 | Suzanne | scrypt:32768:8:1$qXyT99uu4d5F74Yc$531717358dca585dec18c73d4efa1d7b33ea9c338bcfee65aae920395363f41ecd16c32ce71830c0756391bb2f9c342a7c4073d6d7fb75f052b8b7ed7de7656d | admin | True | 2026-07-15 15:14:46.234905+00:00 | 2026-07-15 15:14:46.234905+00:00 |

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

### Example Data

| id | customer_name | full_address | contact_person | phone_number | notes |
| --- | --- | --- | --- | --- | --- |
| 1 | NaN | P.O. Box 26596, Akron, OH, 44319 | NaN | NaN | NaN |
| 2 | 1127 Construction | 1288 Starlight Dr, Akron, OH, 44306 | NaN | NaN | NaN |

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

### Example Data

| id | job_code | job_name | customer | active | source_updated_at | refreshed_at | tax_exempt |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2627 | 6667 | Alcosan N.End Plant Exp (6667) | Mascaro Construction Co., LP | 1 | NULL | 2026-08-26T10:41:50-04:00 | Y |
| 2732 | 6857 | Waltco Parking Lot (6857) | Redcon, LLC | 1 | NULL | 2026-08-26T10:41:50-04:00 | N |

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

### Example Data

| id | job_code | job_name | customer | tax_exempt | active | created_at |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | wendys job | wendys job |  | New | 1 | 2026-04-10T10:15:56 |
| 2 | new job 54 | new job 54 |  | New | 1 | 2026-04-10T10:20:21 |

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

### Example Data

| id | prediction_id | ticket_id | ticket_material | predicted_material | feedback_type | correct_material | feedback_given_at | feedback_given_by |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 260 | 17559 | #304 Recycled Concrete | Broken Concrete | TICKET_MATERIAL_CORRECT | NULL | 2026-08-20 17:00:57.885798+00:00 |  |
| 2 | 255 | 17554 | Other Clean Hard Fill (rubble & mixed material) | Broken Concrete | TICKET_MATERIAL_CORRECT | NULL | 2026-08-20 17:07:31.371660+00:00 |  |

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

### Example Data

| id | ticket_number | ticket_created_at | material_entry | image_url | predicted_label | top1_confidence | top_k_json | raw_output_json | model_name | model_path | inference_ms | status | error_message | created_at | updated_at | ticket_id | predicted_at |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 183 | DT-2026-014810 | 2026-08-20T06:58:58-04:00 | Bulk Sand (pre-screened) OUT | https://mccrakentktsys.blob.core.windows.net/ticket-pdfs/ticket-images/2026/Bulk_Sand__pre-screened__OUT/DT-2026-014810_2026-08-20T06_58_58-04_00.jpg | Broken Concrete | 0.8789611 | [{"label": "Broken Concrete", "confidence": 0.8789610862731934}, {"label": "Other Clean Hard Fill (rubble & mixed material)", "confidence": 0.10446440428495407}, {"label": "Broken Asphalt (or material mixed asphalt)", "confidence": 0.015569521114230156}, {"label": "Slop", "confidence": 0.0010049438569694757}] | {"top1": "Broken Concrete", "top5": [{"label": "Broken Concrete", "confidence": 0.8789610862731934}, {"label": "Other Clean Hard Fill (rubble & mixed material)", "confidence": 0.10446440428495407}, {"label": "Broken Asphalt (or material mixed asphalt)", "confidence": 0.015569521114230156}, {"label": "Slop", "confidence": 0.0010049438569694757}]} | YOLO material classifier | C:\Users\pstanam.MRX\Downloads\OneDrive_2026-02-23\McCraken Ticket System\runs\classify\material_model_runs\starter_classifier-2\weights\best.pt | 8773 | COMPLETED |  | 2026-08-20 16:58:12.293321+00:00 | 2026-08-20 16:58:12.293321+00:00 | 17482 | 2026-08-20 16:58:12.293321+00:00 |
| 184 | DT-2026-014811 | 2026-08-20T07:01:28-04:00 | Bulk Sand (pre-screened) OUT | https://mccrakentktsys.blob.core.windows.net/ticket-pdfs/ticket-images/2026/Bulk_Sand__pre-screened__OUT/DT-2026-014811_2026-08-20T07_01_28-04_00.jpg | Broken Concrete | 0.82334286 | [{"label": "Broken Concrete", "confidence": 0.8233428597450256}, {"label": "Other Clean Hard Fill (rubble & mixed material)", "confidence": 0.13249114155769348}, {"label": "Broken Asphalt (or material mixed asphalt)", "confidence": 0.035166915506124496}, {"label": "Slop", "confidence": 0.008999052457511425}] | {"top1": "Broken Concrete", "top5": [{"label": "Broken Concrete", "confidence": 0.8233428597450256}, {"label": "Other Clean Hard Fill (rubble & mixed material)", "confidence": 0.13249114155769348}, {"label": "Broken Asphalt (or material mixed asphalt)", "confidence": 0.035166915506124496}, {"label": "Slop", "confidence": 0.008999052457511425}]} | YOLO material classifier | C:\Users\pstanam.MRX\Downloads\OneDrive_2026-02-23\McCraken Ticket System\runs\classify\material_model_runs\starter_classifier-2\weights\best.pt | 21 | COMPLETED |  | 2026-08-20 16:58:12.293321+00:00 | 2026-08-20 16:58:12.293321+00:00 | 17483 | 2026-08-20 16:58:12.293321+00:00 |

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

### Example Data

| id | cat | material | direction | axle_1 | tandem | triaxle | axle_4_5 | axle_6 | semi | hydvac | dirt_in | active | cost_per_cy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 41 | #1&2 Recycled Concrete | OUT | 140.0 | 168.0 | 224.0 | 308.0 | 336.0 | 364.0 | 140.0 | nan | True | 28.0 |
| 2 | 33 | #10 Limestone | OUT | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | nan | False | 0.0 |

## Table: `public.materials`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `id` | `bigint` | NO | `nextval('materials_id_seq'::regclass)` |
| 2 | `material_name` | `text` | NO | `` |
| 3 | `active` | `integer` | NO | `1` |

### Example Data

No example data available.

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

### Example Data

| id | epc | truck_number | source | active | created_at | updated_at |
| --- | --- | --- | --- | --- | --- | --- |
| 52 | E28011B0A502006C028C8EFA | TRA111 | serial_assignment | True | 2026-07-29 13:54:00.005499+00:00 | 2026-07-29 13:54:00.005499+00:00 |
| 53 | E28011B0A502006C028C3A9A | RL12 | serial_assignment | True | 2026-07-29 15:11:25.205965+00:00 | 2026-07-29 15:11:25.205965+00:00 |

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

### Example Data

| id | event_type | truck_number | source | message | detected_at | created_at | status | decided_at |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 19 | known_truck_detected | AAA111 | rfid-gate-1 | Known truck appeared: AAA111 | 2026-05-15 20:09:42.304088+00:00 | 2026-05-15 20:09:44.958643+00:00 | denied | 2026-05-15 20:09:47.052854+00:00 |
| 18 | known_truck_detected | ABCD1234 | rfid-gate-1 | Known truck appeared: ABCD1234 | 2026-05-15 20:01:54.680765+00:00 | 2026-05-15 20:01:57.342186+00:00 | denied | 2026-05-15 20:09:53.368383+00:00 |

## Table: `public.rfid_tag_catalog`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `serial_number` | `text` | NO | `` |
| 2 | `epc` | `text` | NO | `` |
| 3 | `active` | `boolean` | NO | `true` |
| 4 | `created_at` | `timestamp with time zone` | NO | `now()` |
| 5 | `updated_at` | `timestamp with time zone` | NO | `now()` |

### Example Data

| serial_number | epc | active | created_at | updated_at |
| --- | --- | --- | --- | --- |
| 1 | E28011B0A502006C028C8E0A | True | 2026-06-22 14:56:23.958483+00:00 | 2026-06-22 14:56:23.958483+00:00 |
| 2 | E28011B0A502006C02872E6A | True | 2026-06-22 14:56:23.958483+00:00 | 2026-06-22 14:56:23.958483+00:00 |

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

### Example Data

| id | serial_number | truck_id | notes | assigned_at | updated_at |
| --- | --- | --- | --- | --- | --- |
| 7 | 38 | 365 | issued 7/23/26 | 2026-07-23 12:18:58.692946+00:00 | 2026-07-23 12:25:16.590123+00:00 |
| 91 | 83 | 659 |  | 2026-08-05 18:33:20.669352+00:00 | 2026-08-05 18:33:20.669352+00:00 |

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

### Example Data

| id | ticket_id | ticket_number | action | actor_user_id | actor_username_snapshot | before_data | after_data | changed_fields | note | created_at |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 15023 | DT-2026-012351 | CREATE | 4 | Mike | {} | {'id': 15023, 'cost': 200.0, 'unit': 'Load', 'notes': '', 'active': True, 'job_id': 2932, 'quantity': 1.0, 'truck_id': 185, 'direction': 'IN', 'image_url': '', 'created_at': '2026-07-15T11:35:11-04:00', 'deleted_at': None, 'tax_exempt': 'N', 'updated_at': '2026-07-15T15:35:12.090526+00:00', 'image_error': '', 'material_id': 10, 'ticket_year': 2026, 'image_status': '', 'ticket_number': 'DT-2026-012351', 'modified_to_id': None, 'ticket_sequence': 12351, 'customer_snapshot': 'Baumann Enterprises, Inc.', 'job_code_snapshot': '15103', 'job_name_snapshot': 'Kenmore Commons (15103)', 'created_by_user_id': 4, 'deleted_by_user_id': None, 'image_generated_at': None, 'updated_by_user_id': 4, 'truck_number_snapshot': 'ABCD1234', 'material_name_snapshot': 'Broken Asphalt (or material mixed asphalt)'} | ['active', 'cost', 'created_at', 'created_by_user_id', 'customer_snapshot', 'direction', 'id', 'image_error', 'image_status', 'image_url', 'job_code_snapshot', 'job_id', 'job_name_snapshot', 'material_id', 'material_name_snapshot', 'notes', 'quantity', 'tax_exempt', 'ticket_number', 'ticket_sequence', 'ticket_year', 'truck_id', 'truck_number_snapshot', 'unit', 'updated_at', 'updated_by_user_id'] | Ticket created | 2026-07-15 15:35:12.090526+00:00 |
| 2 | 15023 | DT-2026-012351 | UPDATE | 6 | Suzanne | {'id': 15023, 'cost': 200.0, 'unit': 'Load', 'notes': '', 'active': True, 'job_id': 2932, 'quantity': 1.0, 'truck_id': 185, 'direction': 'IN', 'image_url': 'https://mccrakentktsys.blob.core.windows.net/ticket-pdfs/ticket-images/2026/Broken_Asphalt__or_material_mixed_asphalt_/DT-2026-012351_2026-07-15T11_35_11-04_00.jpg', 'created_at': '2026-07-15T11:35:11-04:00', 'deleted_at': None, 'tax_exempt': 'N', 'updated_at': '2026-07-15T15:35:12.090526+00:00', 'image_error': '', 'material_id': 10, 'ticket_year': 2026, 'image_status': 'READY', 'ticket_number': 'DT-2026-012351', 'modified_to_id': None, 'ticket_sequence': 12351, 'customer_snapshot': 'Baumann Enterprises, Inc.', 'job_code_snapshot': '15103', 'job_name_snapshot': 'Kenmore Commons (15103)', 'created_by_user_id': 4, 'deleted_by_user_id': None, 'image_generated_at': '2026-07-15T11:35:17-04:00', 'updated_by_user_id': 4, 'truck_number_snapshot': 'ABCD1234', 'material_name_snapshot': 'Broken Asphalt (or material mixed asphalt)'} | {'id': 15023, 'cost': 400.0, 'unit': 'Load', 'notes': '', 'active': True, 'job_id': 2932, 'quantity': 2.0, 'truck_id': 185, 'direction': 'IN', 'image_url': 'https://mccrakentktsys.blob.core.windows.net/ticket-pdfs/ticket-images/2026/Broken_Asphalt__or_material_mixed_asphalt_/DT-2026-012351_2026-07-15T11_35_11-04_00.jpg', 'created_at': '2026-07-15T11:35:00', 'deleted_at': None, 'tax_exempt': 'N', 'updated_at': '2026-07-15T15:37:51.893266+00:00', 'image_error': '', 'material_id': 10, 'ticket_year': 2026, 'image_status': 'READY', 'ticket_number': 'DT-2026-012351', 'modified_to_id': None, 'ticket_sequence': 12351, 'customer_snapshot': 'Baumann Enterprises, Inc.', 'job_code_snapshot': '15103', 'job_name_snapshot': 'Kenmore Commons (15103)', 'created_by_user_id': 4, 'deleted_by_user_id': None, 'image_generated_at': '2026-07-15T11:35:17-04:00', 'updated_by_user_id': 6, 'truck_number_snapshot': 'ABCD1234', 'material_name_snapshot': 'Broken Asphalt (or material mixed asphalt)'} | ['cost', 'created_at', 'quantity', 'updated_at', 'updated_by_user_id'] | Ticket edited | 2026-07-15 15:37:51.893266+00:00 |

## Table: `public.ticket_sequence`

### Schema

| # | Column | Data Type | Nullable | Default |
| --- | --- | --- | --- | --- |
| 1 | `ticket_year` | `integer` | NO | `` |
| 2 | `last_value` | `integer` | NO | `` |

### Example Data

| ticket_year | last_value |
| --- | --- |
| 2026 | 15199 |

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

### Example Data

| id | ticket_number | ticket_year | ticket_sequence | direction | created_at | job_id | job_code_snapshot | job_name_snapshot | customer_snapshot | truck_id | truck_number_snapshot | material_id | material_name_snapshot | quantity | unit | notes | pdf_path | pdf_blob | tax_exempt | cost | active | modified_to_id | image_url | image_status | image_error | image_generated_at | created_by_user_id | updated_by_user_id | deleted_by_user_id | updated_at | deleted_at | voided_at | created_by | updated_by |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2673 | DT-2026-000001 | 2026 | 1 | IN | 2026-01-02T09:58:00 | NULL | Parkview Dr. | Parkview Dr. | Middleton Mechanical, LLC | 110 | MID902 | 18 | other clean hard fill (rubble & mixed material) | 1.0 | Load |  | NULL | NULL | N | 75.0 | True | NULL |  |  |  | NULL | NULL | NULL | NULL | 2026-07-15 12:29:36.264146+00:00 | NULL | NULL |  | NULL |
| 2674 | DT-2026-000002 | 2026 | 2 | IN | 2026-01-05T07:49:00 | 3608 | 2720 | Woodhill Roads PH1 (2720) | Petty Group LLC | 54 | DEV5 | 18 | other clean hard fill (rubble & mixed material) | 1.0 | Load |  | NULL | NULL | N | 165.0 | True | NULL |  |  |  | NULL | NULL | NULL | NULL | 2026-07-15 12:29:36.264146+00:00 | NULL | NULL |  | NULL |

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

### Example Data

No example data available.

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
| 9 | `created_by` | `text` | YES | `` |
| 10 | `created_by_user_id` | `bigint` | YES | `` |
| 11 | `updated_by_user_id` | `bigint` | YES | `` |
| 12 | `updated_by` | `text` | YES | `` |

### Example Data

| id | truck_number | trucking_company | notes | truck_size | phone | license_plate | active | created_by | created_by_user_id | updated_by_user_id | updated_by |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 205 | MIKE108 | Legacy Data (Unknown Hauler) | NULL | TriAxle | NULL | NULL | True | NULL | NULL | NULL | NULL |
| 206 | POND15 | Legacy Data (Unknown Hauler) | NULL | Tandem | NULL | NULL | True | NULL | NULL | NULL | NULL |

