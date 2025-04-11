from typing import Annotated
from mcp.server.fastmcp import FastMCP
import os
from dotenv import load_dotenv
from clickhouse_driver import Client

load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))  # Note: Driver uses port 9000 by default
CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USERNAME", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")

# Set up the MCP server with a domain name
mcp = FastMCP("clickhouse")

# Initialize the ClickHouse client
client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USERNAME,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE
)


@mcp.tool()
def run_clickhouse_query(prompt: Annotated[str, "A SQL query to run on the ClickHouse database."]) -> str:
    """
        Execute a SQL query and return formatted results which supposts only SELECT queries.
        Update and Create or Delete Queries should be rejected right away 
    """
    try:
        result = client.execute(prompt)
        
        if not result:
            return "No results returned"

        output = []
        
        # Get column names for SELECT queries
        if prompt.strip().upper().startswith("SELECT"):
            prompt_with_limit = prompt + " LIMIT 0"
            columns = [col[0] for col in client.execute(f"DESC ({prompt_with_limit})")]
            output.append(" | ".join(columns))
            output.append("-" * 50)
        
        # Format the rows
        for row in result:
            output.append(" | ".join(str(item) for item in row))

        formatted_result = "\n".join(output)
        return formatted_result if formatted_result else "Query executed successfully but returned no data"
    
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
def list_tables() -> str:
    """List all tables in the current ClickHouse database."""
    try:
        print(f"Listing tables in database: {CLICKHOUSE_DATABASE}")
        # Use execute instead of query
        result = client.execute("SHOW TABLES")
        tables = [row[0] for row in result]
        return "📦 Tables:\n" + "\n".join(f"- {t}" for t in tables)
    except Exception as e:
        return f"❌ Error fetching tables: {str(e)}"


@mcp.tool()
def describe_table(
    table_name: Annotated[str, "Name of the ClickHouse table to describe."]
) -> str:
    """Describe the schema of a ClickHouse table."""
    try:
        # Use execute instead of query
        result = client.execute(f"DESCRIBE TABLE {table_name}")
        output = ["Column | Type | Default Type | Default Expression"]
        output.append("-" * 60)
        for row in result:
            output.append(" | ".join(str(x) for x in row[:4]))
        return "\n".join(output)
    except Exception as e:
        return f"❌ Error describing table: {str(e)}"


# @mcp.tool()
# def format_table_output(content: Annotated[str, "The tabular data converted to human friendly text with proper spacing and listing."]) -> str:
#     """The tabular data converted to human friendly text with proper spacing and listing."""
#     try:
#         return content
#     except Exception as e:
#         return f"Error: {str(e)}"


if __name__ == "__main__":
    mcp.run(transport="stdio")