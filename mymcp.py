import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Dict

import httpx
from gql import Client, gql
from gql.transport.httpx import HTTPXAsyncTransport
from mcp.server.fastmcp import FastMCP,Context

# --- 1. Clean Configuration Management ---
@dataclass
class AppConfig:
    """A simple and clean container for application configuration."""
    username: str
    password: str
    url: str
    verify_tls: bool = True

    @classmethod
    def from_env(cls ):
        """Loads configuration from environment variables, raising a clear error if any are missing."""
        try:
            return cls(
                username=os.environ["USERNAME"],
                password=os.environ["PASSWORD"],
                url=os.environ["URL"],
                verify_tls=os.getenv("TLSVERIFY", "true").lower() in ("true", "1"),
            )
        except KeyError as e:
            raise EnvironmentError(f"A required environment variable is missing: {e}") from e


@dataclass
class Quota:
    inodeHard: int =0
    inodeSoft: int =0
    kbyteHard: int =0
    kbyteSoft: int =0
    kbyteUsed: int =0 
    inodeUsed: int =0

# --- 2. Elegant and Corrected Asynchronous GraphQL Client ---
class GraphQLClient:
    """An async client that manages authentication and executes GraphQL queries."""

    def __init__(self, config: AppConfig):
        self.config = config
        self._session_cookie: str | None = None
        self._session_expires_at: float = 0
        # The transport is now an instance variable, created after a successful login.
        self._transport: HTTPXAsyncTransport | None = None

    async def _login(self) -> None:
        """Asynchronously logs in using httpx and creates the GraphQL transport."""
        # Use a temporary httpx client for the login process.
        async with httpx.AsyncClient(verify=self.config.verify_tls ) as login_client:
            try:
                resp = await login_client.post(
                    f"{self.config.url}/session",
                    json={"username": self.config.username, "password": self.config.password},
                )
                resp.raise_for_status()  # Checks for HTTP errors (e.g., 401, 500)
                
                cookie = resp.cookies.get("sessionid")
                if not cookie:
                    raise ConnectionError("Login failed: 'sessionid' cookie not found in response.")

                self._session_cookie = cookie
                self._session_expires_at = time.time() + 3600  # Assume session is valid for 1 hour

                # After a successful login, create or update the transport.
                # This is the key fix for the TypeError.
                self._transport = HTTPXAsyncTransport(
                    url=f"{self.config.url}/graphql",
                    headers={"Cookie": f"sessionid={self._session_cookie}"},
                    verify=self.config.verify_tls,
                    timeout=30.0
                )

            except httpx.RequestError as e:
                raise ConnectionError(f"Could not connect to the authentication endpoint: {e}") from e

    async def execute(self, query: str, variables: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Executes a GraphQL query, handling login automatically if needed."""
        if time.time() >= self._session_expires_at or self._transport is None:
            await self._login()
        
        # Initialize the gql client with the created transport.
        gql_client = Client(transport=self._transport)
        
        return await gql_client.execute_async(query, variable_values=variables)

# --- 3. Service and Tool Initialization ---
# Define GQL queries as constants to keep tool functions clean.
LIST_USERS_QUERY = gql("""
query{
  user {
    list {
      id
      name
    }
  }
}
""")

GET_ERRORS_QUERY = gql("""
query($number: Int!){
  alert {
    list(limit: $number, dir: DESC, severity: Error) {
      data {
        id
        message
      }
    }
  }
}
""")

LIST_TENANTS_QUERY = gql("""
query{
  tenant {
    list(refresh: true) {
      name
      fileset {
        path
        readonly
      }
      idOffset
      nids {
        startNid
        endNid
      }
    }
  }
}
""")

LIST_QUOTA_QUERY =  gql("""
query{
  	quota{
list{
  id
  quotas{
    projids{
      kbytes {
        id
        quota{
          hard
          soft
          granted
        }
      }
      inodes {
        id
        quota{
          hard
          soft
          granted
        }
      }
    }
  }
}
}
}
""")

CREATE_TENANT_MUTATION = gql("""
    mutation CreateTenant($name: String!) {
      tenant {
        create(action: execute,name: $name, nids: [], quota: {}) {
          ... on Command { id name state }
        }
      }
    }
""")

DELETE_USER_MUTATION = gql("""
    mutation  DeleteUser($name: String!){
      user {
        destroy(name: $name) 
      } 
    }       
""")      

CHECK_STATEMACHINE = gql("""    
query CheckStateMachine($id: Int!){
  	stateMachine{
getCmdSummary (id: $id){
  name
  state
  failureReason
} 
}
}
""")  

CHANGE_QUOTA_MUTATION = gql("""
mutation ChangeQuota($name: String!, $quota: SetQuotaLimit!){
  	tenant{
 setQuota (action: execute,name:$name,quota:$quota){
 ... on Command {
  id
}
}
}
""")

DESTROY_TENANT_MUTATION = gql("""
    mutation DestroyTenant($name: String!) {
      tenant {
        destroy(action: execute, name: $name, destroyData: false) {
          ... on Command {
            id
          }
        }
      }
    }
""")

ADD_NIDS_MUTATION= gql("""
mutation AddNids($name: String!, $nids: [String!]!){
  tenant{
    addNids(action:execute,name:$name,nids: $nids){
      ... on Command {
        id
      }
    }
  }
}
""")

REMOVE_NIDS_MUTATION= gql("""
mutation RemoveNids($name: String!, $nids: [String!]!){
  tenant{
    removeNids(action:execute,name:$name,nids: $nids){
      ... on Command {
        id
      }
    }
  }
}
""")

CREATE_TENANT_MUTATION= gql("""
mutation CreateTenant($name: String!, $nids: [String!],$quota:SetQuotaLimit){
  tenant{
    create(action:execute,name:$name,nids: $nids,quota:$quota){
      ... on Command {
        id
      }
    }
  }
}
""")

# Initialize the core components of the application.
config = AppConfig.from_env()
gql_client = GraphQLClient(config)
mcp = FastMCP("exascaler")

# Tool definitions are now extremely simple and intuitive.
@mcp.tool()
async def list_users() -> Dict[str, Any]:
    """Lists all users."""
    return await gql_client.execute(LIST_USERS_QUERY)

@mcp.tool()
async def delete_user(name: str) -> bool:
    """Delete a existing user."""
    return await gql_client.execute(DELETE_USER_MUTATION, variables={"name": name})

@mcp.tool()
async def get_errors(number: int = 10) -> Dict[str, Any]:
    """Gets the most recent error alerts , 10 by default."""
    return await gql_client.execute(GET_ERRORS_QUERY, variables={"number": number})

@mcp.tool()
async def list_tenants() -> Dict[str, Any]:
    """Lists all tenants and its quota."""
    tenants =  await gql_client.execute(LIST_TENANTS_QUERY)
    tenant_quotas = {tenant["idOffset"]:Quota() for tenant in tenants["tenant"]["list"]}
    quota = await gql_client.execute(LIST_QUOTA_QUERY)
    for i in quota["quota"]["list"]:
      for j in i["quotas"]["projids"]["kbytes"]:
        if j["id"] in tenant_quotas:
            tenant_quotas[j["id"]].kbyteHard = j["quota"]["hard"]
            tenant_quotas[j["id"]].kbyteSoft = j["quota"]["soft"]
            tenant_quotas[j["id"]].kbyteUsed = j["quota"]["granted"]
      for k in i["quotas"]["projids"]["inodes"]:
        if k["id"] in tenant_quotas:
          tenant_quotas[k["id"]].inodeHard = k["quota"]["hard"]
          tenant_quotas[k["id"]].inodeSoft = k["quota"]["soft"] 
          tenant_quotas[k["id"]].inodeUsed = k["quota"]["granted"]

    for tenant in tenants["tenant"]["list"]:
      tenant["quota"] = tenant_quotas[tenant["idOffset"]]
    return tenants

async def _check_state_machine(id: int, ctx: Context) -> Dict[str, Any]:
    """Check state machine status."""
    while True:
        result = await gql_client.execute(CHECK_STATEMACHINE, variables={"id": id})
        summary = result["stateMachine"]["getCmdSummary"]
        if summary["state"] in ["failed", "canceled","skipped","completed"]:
            return result
        await ctx.info(f'{summary["name"]} is still ongoing.')
        await asyncio.sleep(3)    


@mcp.tool()
async def modify_tenant_quota(name: str, ctx: Context, inodeHard: str|None = None, inodeSoft: str|None = None, kbyteHard: str|None =None, kbyteSoft: str|None=None) -> Dict[str, Any]:
    """Modify tenant quota."""
    quota_input = {}
    if inodeHard:
        quota_input["inodeHard"] = inodeHard
    if inodeSoft:
        quota_input["inodeSoft"] = inodeSoft
    if kbyteHard:
        quota_input["kbyteHard"] = kbyteHard
    if kbyteSoft:
        quota_input["kbyteSoft"] = kbyteSoft
    result=await gql_client.execute(CHANGE_QUOTA_MUTATION, variables={"name": name, "quota": quota_input})
    id = result["tenant"]["setQuota"]["id"]
    await ctx.info(f"Quota change for {name} started, id: {id}")
    return await _check_state_machinecheck_state_machine(id,ctx)

@mcp.tool()
async def destroy_tenant(name: str, ctx: Context,confirm: bool = False) -> Dict[str, Any]:
    """Destroy a tenant,retain its data."""
    await ctx.warning(f"⚠️ Warning: You are about to delete tenant '{name}'. This action is irreversible!")
    if not confirm:
        await ctx.info("Operation cancelled. To confirm deletion, set 'confirm' parameter to True.")
        return {"status": "cancelled", "message": "Deletion not confirmed. Set 'confirm=True' to proceed."}

    result = await gql_client.execute(DESTROY_TENANT_MUTATION,variables={"name": name})

    id = result["tenant"]["destroy"]["id"]
    await ctx.info(f"Tenant '{name}' deletion operation has started, data will be retained")
    return await _check_state_machine(id,ctx)

@mcp.tool()
async def add_nids_to_tenant(name: str, nids: list[str], ctx: Context) -> Dict[str, Any]:

    """Adds NIDs (Network Identifiers) or NID ranges to a specified tenant.
    Each element in `nids` should be a single NID (e.g., '10.20.40.1@o2ib') 
    or a continuous NID range (e.g., '10.20.40.[0-254]@o2ib')."""

    await ctx.info(f"Add nid/nid ranges to tenant. each element in nids should be a single nid (e.g., '10.20.40.1@o2ib')  or a continous nid range (e.g., '10.20.40.[0-254]@o2ib').")
    result = await gql_client.execute(ADD_NIDS_MUTATION,variables={"name": name,"nids": nids})
    id = result["tenant"]["addNids"]["id"]
    await ctx.info(f"Add nids to tenant '{name}' operation has started")
    return await _check_state_machine(id,ctx)


@mcp.tool()
async def remove_nids_from_tenant(name: str, nids: list[str], ctx: Context) -> Dict[str, Any]:

    """Removes NIDs (Network Identifiers) or NID ranges from a specified tenant.
    Each element in `nids` should be a single NID (e.g., '10.20.40.1@o2ib') 
    or a continuous NID range (e.g., '10.20.40.[0-254]@o2ib')."""

    await ctx.warning(f"Remove nid/nid ranges from tenant. each element in nids should be a single nid (e.g., '10.20.40.1@o2ib')  or a continous nid range (e.g., '10.20.40.[0-254]@o2ib').")
    result = await gql_client.execute(REMOVE_NIDS_MUTATION,variables={"name": name,"nids": nids})
    id = result["tenant"]["removeNids"]["id"]
    await ctx.info(f"Remove nids from tenant '{name}' operation has started") 
    return await _check_state_machine(id,ctx)

@mcp.tool()
async def create_tenant(name: str,ctx: Context, nids: list[str]|None = None,inodeHard: str|None = None, inodeSoft: str|None = None, kbyteHard: str|None =None, kbyteSoft: str|None=None) -> Dict[str, Any]:

    """Creates a new tenant with optional NIDs and quota settings.
    Each element in `nids` should be a single NID (e.g., '10.20.40.1@o2ib') 
    or a continuous NID range (e.g., '10.20.40.[0-254]@o2ib')."""

    quota_input = {}
    if inodeHard:
        quota_input["inodeHard"] = inodeHard
    if inodeSoft:
        quota_input["inodeSoft"] = inodeSoft
    if kbyteHard:
        quota_input["kbyteHard"] = kbyteHard
    if kbyteSoft:
        quota_input["kbyteSoft"] = kbyteSoft
    if nids is None:
        nids = []
    await ctx.info(f"Create a tenant. each element in nids should be a single nid (e.g., '10.20.40.1@o2ib') or a continous nid range (e.g., '10.20.40.[0-254]@o2ib').")
    result = await gql_client.execute(CREATE_TENANT_MUTATION,variables={"name": name,"nids": nids,"quota": quota_input})
    id = result["tenant"]["create"]["id"]
    await ctx.info(f"Create tenant '{name}' operation has started") 
    return await _check_state_machine(id,ctx)

