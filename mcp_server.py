from mcp.server import MCPServer

from app import create_db_connection
from ticket_service import search_tickets_service


mcp = MCPServer(
    "McCraken Ticket System"
)


@mcp.tool()
def search_ticket(
    ticket_number: str = "",
    truck: str = "",
    job: str = "",
    customer: str = "",
    material: str = "",
    date_from: str = "",
    date_to: str = ""
) -> dict:
    """
    Search active tickets.

    Tickets can be searched using ticket number,
    truck number, job code, customer name,
    material name, or date range.

    Dates should use YYYY MM DD format.
    """

    db = create_db_connection()

    tickets = search_tickets_service(
        db=db,
        ticket_number=ticket_number,
        truck=truck,
        job=job,
        customer=customer,
        material=material,
        date_from=date_from,
        date_to=date_to
    )

    cleaned_tickets = []

    for ticket in tickets:

        cleaned_ticket = dict(ticket)

        created_at = cleaned_ticket.get("created_at")
        if created_at is not None:
            if hasattr(created_at, "isoformat"):
                cleaned_ticket["created_at"] = created_at.isoformat()
            else:
                cleaned_ticket["created_at"] = str(created_at)

        cleaned_tickets.append(
            cleaned_ticket
        )

    return {
        "count": len(cleaned_tickets),
        "tickets": cleaned_tickets
    }


if __name__ == "__main__":
    mcp.run()