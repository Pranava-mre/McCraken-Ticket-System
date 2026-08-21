def search_tickets_service(
    db,
    ticket_number="",
    truck="",
    job="",
    customer="",
    material="",
    date_from="",
    date_to="",
    limit=200
):
    query = """
        SELECT
            id,
            ticket_number,
            created_at,
            direction,
            job_code_snapshot,
            tax_exempt,
            customer_snapshot,
            truck_number_snapshot,
            material_name_snapshot,
            cost,
            image_url,
            image_status,
            image_error,
            CASE
                WHEN pdf_blob IS NOT NULL
                     OR COALESCE(pdf_path, '') <> ''
                THEN TRUE
                ELSE FALSE
            END AS has_pdf
        FROM tickets
        WHERE COALESCE(active, TRUE) = TRUE
    """

    params = []

    if ticket_number:
        query += " AND ticket_number ILIKE %s"
        params.append(f"%{ticket_number}%")

    if truck:
        query += " AND truck_number_snapshot ILIKE %s"
        params.append(f"%{truck}%")

    if job:
        query += " AND job_code_snapshot ILIKE %s"
        params.append(f"%{job}%")

    if customer:
        query += " AND customer_snapshot ILIKE %s"
        params.append(f"%{customer}%")

    if material:
        query += " AND material_name_snapshot ILIKE %s"
        params.append(f"%{material}%")

    if date_from:
        query += " AND date(created_at) >= date(%s)"
        params.append(date_from)

    if date_to:
        query += " AND date(created_at) <= date(%s)"
        params.append(date_to)

    query += """
        ORDER BY created_at DESC, id DESC
        LIMIT %s
    """

    params.append(limit)

    with db.cursor() as cursor:
        cursor.execute(query, tuple(params))
        tickets = cursor.fetchall()

    return tickets