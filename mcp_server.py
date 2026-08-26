from mcp.server import MCPServer

from services.db_service import create_db_connection
from services.manual_search_service import search_manual_service
from services.ticket_service import search_tickets_service


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


@mcp.tool()
def search_user_guide(
    query: str
):
    """
    Search the McCracken Window Ticket user manual.

    Use this tool for questions about how to use the
    ticketing system.

    Examples include:

    creating tickets
    editing tickets
    voiding tickets
    printing tickets
    searching tickets
    adding trucks
    customers
    materials
    jobs
    direction IN or OUT
    reports
    daily reports
    ticket procedures
    system instructions
    troubleshooting

    This tool does not return live ticket data.
    """

    return search_manual_service(
        query=query,
        limit=3
    )

if __name__ == "__main__":
    mcp.run()