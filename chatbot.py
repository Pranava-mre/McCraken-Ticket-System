import os
import json
import asyncio

from dotenv import load_dotenv
from openai import OpenAI

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


load_dotenv()

client = OpenAI(
    base_url=os.environ["AZURE_OPENAI_ENDPOINT"],
    api_key=os.environ["AZURE_OPENAI_API_KEY"]
)

MODEL = os.environ["AZURE_OPENAI_MODEL"]

# SYSTEM_PROMPT = """
# You are the AI assistant for the Mr. Excavator ticketing system.

# Your scope is strictly limited to:
# 1. Ticket information
# 2. Truck information
# 3. Customer information
# 4. Job information
# 5. Material information
# 6. RFID related information
# 7. Reports available through the connected MCP tools
# 8. Questions about the ticketing system itself

# Only assist with the ticketing system and connected MCP tools.

# Keep responses concise.
# Use at most one MCP tool for each user request.

# Prefer filtered queries over broad queries.

# If a request could return a large number of records, use the narrowest available filters.

# Do not request additional records merely to provide a more detailed answer.

# If the available result is too large, tell the user to narrow the request.

# If a tool result says that it was truncated or that the result set is too large:
# 1. Do not attempt to summarize the missing records.
# 2. Tell the user that the result is larger than the application's processing limit.
# 3. Provide only the information available in the truncated result.
# 4. Suggest narrowing the request using filters such as date, customer, truck, job, or material.
# 5. Never claim that a truncated result represents all records.

# Do not help with unrelated requests such as:
# writing emails,
# resumes,
# general coding,
# homework,
# travel,
# personal advice,
# general knowledge,
# creative writing,
# financial advice,
# medical questions,
# or any other topic unrelated to this ticketing system.

# If the request is outside your allowed scope, reply only with:
# "I can only assist with questions related to the Mr. Excavator ticketing system."

# Do not explain your general AI capabilities.
# Do not mention being able to help with unrelated tasks.
# Do not invent ticketing data.
# If the requested information is not available through the provided tools, say that the information is not available.
# """
SYSTEM_PROMPT = """
You are the AI assistant for the McCracken ticketing system.

Only assist with:
1. Tickets
2. Trucks
3. Customers
4. Jobs
5. Materials
6. RFID
7. Reports
8. The ticketing system and connected MCP tools

Rules:
1. Keep responses concise.
2. Use at most one MCP tool per request.
3. Prefer specific, filtered queries over broad queries.
4. Never invent system data.
5. If information is unavailable through the tools, say it is unavailable.
6. If a tool result is truncated or too large, only use the returned information. Tell the user the result exceeds the processing limit and suggest narrowing by date, customer, truck, job, or material.
7. Never imply that a truncated result contains all records.
8. Do not assist with anything outside the ticketing system.

For any unrelated request, reply only:
"I can only assist with questions related to the McCracken ticketing system."
"""
MAX_TOOL_CALLS = 3
MAX_TOOL_OUTPUT_CHARS = 8000
DEFAULT_SEARCH_LIMIT = 25
MAX_OUTPUT_TOKENS = 500
REASONING_EFFORT = "low"
def prepare_tool_output(result) -> str:
    if result.structured_content:
        data = result.structured_content

        text = json.dumps(data, default=str)

        if len(text) <= MAX_TOOL_OUTPUT_CHARS:
            return text

        return json.dumps({
            "truncated": True,
            "message": (
                "The result set is too large to process in one request. "
                "Ask the user to narrow the query."
            )
        })

    text = "\n".join(
        content.text
        for content in result.content
        if hasattr(content, "text")
    )
    return limit_tool_output(text)

def limit_tool_output(tool_output_text: str) -> str:
    if len(tool_output_text) <= MAX_TOOL_OUTPUT_CHARS:
        return tool_output_text
    preview = tool_output_text[:MAX_TOOL_OUTPUT_CHARS]

    return f"""
    TOOL RESULT TRUNCATED

    The tool returned more data than this application allows the model to process.

    Only the first portion is included below.

    {preview}

    Do not claim this is the complete result.
    Tell the user the result set is too large and ask them to narrow the query.
    """
def convert_mcp_tools_to_openai(mcp_tools):
    openai_tools = []

    for tool in mcp_tools:
        openai_tools.append(
            {
                "type": "function",
                "name": tool.name,
                "description": tool.description or "",
                "parameters": tool.input_schema
            }
        )

    return openai_tools


async def process_message(user_message: str) -> str:
    message = str(user_message or "").strip()
    if not message:
        return "Please enter a question."

    server_params = StdioServerParameters(
        command="python",
        args=["mcp_server.py"]
    )

    async with stdio_client(server_params) as streams:
        read_stream, write_stream = streams

        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            tools_response = await session.list_tools()
            openai_tools = convert_mcp_tools_to_openai(tools_response.tools)

            response = client.responses.create(
                model=MODEL,
                instructions=SYSTEM_PROMPT,
                input=message,
                tools=openai_tools,
                    reasoning={
                        "effort": REASONING_EFFORT
                    },
                    # max_output_tokens=MAX_OUTPUT_TOKENS
            )

            for item in response.output:
                if item.type != "function_call":
                    continue

                arguments = json.loads(item.arguments)
                result = await session.call_tool(item.name, arguments)
                tool_output_text = prepare_tool_output(result)
                final_response = client.responses.create(
                    model=MODEL,
                    previous_response_id=response.id,
                    instructions=SYSTEM_PROMPT,
                    input=[
                        {
                            "type": "function_call_output",
                            "call_id": item.call_id,
                            "output": tool_output_text
                        }
                    ],
                    tools=openai_tools,
                        reasoning={
                            "effort": REASONING_EFFORT
                        },
                    # max_output_tokens=MAX_OUTPUT_TOKENS
                )
                return final_response.output_text

            return response.output_text


async def main():
    while True:
        user_message = input("\nYou: ")
        if user_message.lower() in ["exit", "quit"]:
            break
        print("\nAssistant:")
        print(await process_message(user_message))


if __name__ == "__main__":
    asyncio.run(main())