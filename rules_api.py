from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
import json
import os
from enum import Enum

# Initialize FastAPI app
app = FastAPI(
    title="Rules Management API",
    description="A comprehensive API for managing business rules with create, view, modify, and approve operations",
    version="1.0.0",
    docs_url="/docs",  # Swagger UI endpoint
    redoc_url="/redoc"  # ReDoc endpoint
)

# Simple file-based storage (replaces MongoDB for POC)
RULES_FILE = "rules_storage.json"

# Enums for rule status
class RuleStatus(str, Enum):
    DRAFT = "draft"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    INACTIVE = "inactive"

# Pydantic models for request/response validation
class RuleData(BaseModel):
    name: str = Field(..., description="Name of the rule", example="Email Validation Rule")
    description: str = Field(..., description="Description of the rule", example="Validates email format for user registration")
    conditions: Dict[str, Any] = Field(..., description="Rule conditions as JSON", example={"email_regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"})
    actions: Dict[str, Any] = Field(..., description="Actions to take when rule matches", example={"action": "validate", "error_message": "Invalid email format"})
    priority: int = Field(default=1, description="Rule priority (higher number = higher priority)", example=1)
    tags: List[str] = Field(default=[], description="Tags for categorizing rules", example=["validation", "email"])

class Rule(BaseModel):
    id: str = Field(..., description="Unique rule identifier")
    data: RuleData
    status: RuleStatus = Field(default=RuleStatus.DRAFT, description="Current status of the rule")
    created_at: datetime = Field(..., description="Rule creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    created_by: str = Field(..., description="User who created the rule", example="john.doe")
    approved_by: Optional[str] = Field(None, description="User who approved the rule")
    approved_at: Optional[datetime] = Field(None, description="Approval timestamp")

class RuleUpdate(BaseModel):
    name: Optional[str] = Field(None, description="Updated name of the rule")
    description: Optional[str] = Field(None, description="Updated description")
    conditions: Optional[Dict[str, Any]] = Field(None, description="Updated conditions")
    actions: Optional[Dict[str, Any]] = Field(None, description="Updated actions")
    priority: Optional[int] = Field(None, description="Updated priority")
    tags: Optional[List[str]] = Field(None, description="Updated tags")

class ApprovalRequest(BaseModel):
    approved: bool = Field(..., description="Whether to approve (True) or reject (False) the rule")
    approved_by: str = Field(..., description="Username of the approver", example="jane.manager")
    comments: Optional[str] = Field(None, description="Approval/rejection comments")

# Utility functions for file storage
def load_rules() -> Dict[str, Rule]:
    """Load rules from JSON file"""
    if not os.path.exists(RULES_FILE):
        return {}
    
    try:
        with open(RULES_FILE, 'r') as f:
            data = json.load(f)
            # Convert dict back to Rule objects
            rules = {}
            for rule_id, rule_data in data.items():
                # Parse datetime strings back to datetime objects
                rule_data['created_at'] = datetime.fromisoformat(rule_data['created_at'])
                rule_data['updated_at'] = datetime.fromisoformat(rule_data['updated_at'])
                if rule_data.get('approved_at'):
                    rule_data['approved_at'] = datetime.fromisoformat(rule_data['approved_at'])
                
                rules[rule_id] = Rule(**rule_data)
            return rules
    except Exception as e:
        print(f"Error loading rules: {e}")
        return {}

def save_rules(rules: Dict[str, Rule]):
    """Save rules to JSON file"""
    try:
        # Convert Rule objects to dict for JSON serialization
        data = {}
        for rule_id, rule in rules.items():
            rule_dict = rule.dict()
            # Convert datetime objects to ISO format strings
            rule_dict['created_at'] = rule_dict['created_at'].isoformat()
            rule_dict['updated_at'] = rule_dict['updated_at'].isoformat()
            if rule_dict.get('approved_at'):
                rule_dict['approved_at'] = rule_dict['approved_at'].isoformat()
            data[rule_id] = rule_dict
        
        with open(RULES_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Error saving rules: {e}")
        raise HTTPException(status_code=500, detail="Failed to save rules")

def generate_rule_id() -> str:
    """Generate a unique rule ID"""
    import uuid
    return f"rule_{uuid.uuid4().hex[:8]}"

# API Endpoints

@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with links to API documentation"""
    return """
    <html>
        <head>
            <title>Rules Management API</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }
                .container { background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                h1 { color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }
                .links { margin: 20px 0; }
                .links a { display: inline-block; margin: 10px 15px 10px 0; padding: 12px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; }
                .links a:hover { background: #0056b3; }
                .features { margin: 20px 0; }
                .features li { margin: 8px 0; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🔧 Rules Management API</h1>
                <p>Welcome to the Rules Management System API. This service provides comprehensive rule management capabilities with automatic documentation and testing interface.</p>
                
                <div class="links">
                    <a href="/docs" target="_blank">📚 Swagger UI (Interactive API Testing)</a>
                    <a href="/redoc" target="_blank">📖 ReDoc Documentation</a>
                </div>
                
                <h2>Features</h2>
                <ul class="features">
                    <li>✅ Create new business rules with JSON-based conditions and actions</li>
                    <li>📋 View all rules with filtering and status-based queries</li>
                    <li>✏️ Modify existing rules with partial updates</li>
                    <li>✔️ Approve or reject rules with approval workflow</li>
                    <li>🗂️ File-based storage (easily replaceable with MongoDB)</li>
                    <li>🏷️ Tag-based rule organization and searching</li>
                    <li>📊 Rule priority management</li>
                </ul>
                
                <h2>Quick Start</h2>
                <p>Click on <strong>Swagger UI</strong> above to start testing the API endpoints immediately!</p>
            </div>
        </body>
    </html>
    """

@app.post("/rules", response_model=Rule, summary="Create a new rule", tags=["Rule Management"])
async def create_rule(rule_data: RuleData, created_by: str = Query(..., description="Username of the rule creator")):
    """
    Create a new business rule with the specified conditions and actions.
    
    The rule will be created in 'draft' status and can be modified before approval.
    """
    rules = load_rules()
    
    rule_id = generate_rule_id()
    now = datetime.now()
    
    new_rule = Rule(
        id=rule_id,
        data=rule_data,
        status=RuleStatus.DRAFT,
        created_at=now,
        updated_at=now,
        created_by=created_by
    )
    
    rules[rule_id] = new_rule
    save_rules(rules)
    
    return new_rule

@app.get("/rules", response_model=List[Rule], summary="Get all rules", tags=["Rule Management"])
async def get_rules(
    status: Optional[RuleStatus] = Query(None, description="Filter rules by status"),
    tag: Optional[str] = Query(None, description="Filter rules by tag"),
    created_by: Optional[str] = Query(None, description="Filter rules by creator")
):
    """
    Retrieve all rules with optional filtering by status, tag, or creator.
    """
    rules = load_rules()
    result = list(rules.values())
    
    # Apply filters
    if status:
        result = [rule for rule in result if rule.status == status]
    
    if tag:
        result = [rule for rule in result if tag in rule.data.tags]
    
    if created_by:
        result = [rule for rule in result if rule.created_by == created_by]
    
    # Sort by priority (descending) then by creation date
    result.sort(key=lambda x: (-x.data.priority, x.created_at))
    
    return result

@app.get("/rules/{rule_id}", response_model=Rule, summary="Get a specific rule", tags=["Rule Management"])
async def get_rule(rule_id: str):
    """
    Retrieve a specific rule by its ID.
    """
    rules = load_rules()
    
    if rule_id not in rules:
        raise HTTPException(status_code=404, detail=f"Rule with ID '{rule_id}' not found")
    
    return rules[rule_id]

@app.put("/rules/{rule_id}", response_model=Rule, summary="Update a rule", tags=["Rule Management"])
async def update_rule(rule_id: str, rule_update: RuleUpdate):
    """
    Update an existing rule with new data. Only provided fields will be updated.
    
    Note: Rules in 'approved' status will be moved back to 'draft' status when modified.
    """
    rules = load_rules()
    
    if rule_id not in rules:
        raise HTTPException(status_code=404, detail=f"Rule with ID '{rule_id}' not found")
    
    rule = rules[rule_id]
    
    # Update only provided fields
    update_data = rule_update.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(rule.data, field, value)
    
    # Update metadata
    rule.updated_at = datetime.now()
    
    # If rule was approved, move back to draft status when modified
    if rule.status == RuleStatus.APPROVED:
        rule.status = RuleStatus.DRAFT
        rule.approved_by = None
        rule.approved_at = None
    
    rules[rule_id] = rule
    save_rules(rules)
    
    return rule

@app.post("/rules/{rule_id}/approve", response_model=Rule, summary="Approve or reject a rule", tags=["Rule Approval"])
async def approve_rule(rule_id: str, approval: ApprovalRequest):
    """
    Approve or reject a rule. This moves the rule to 'approved' or 'rejected' status.
    """
    rules = load_rules()
    
    if rule_id not in rules:
        raise HTTPException(status_code=404, detail=f"Rule with ID '{rule_id}' not found")
    
    rule = rules[rule_id]
    
    if rule.status not in [RuleStatus.DRAFT, RuleStatus.PENDING_APPROVAL]:
        raise HTTPException(status_code=400, detail=f"Rule is in '{rule.status}' status and cannot be approved/rejected")
    
    # Update approval status
    if approval.approved:
        rule.status = RuleStatus.APPROVED
    else:
        rule.status = RuleStatus.REJECTED
    
    rule.approved_by = approval.approved_by
    rule.approved_at = datetime.now()
    rule.updated_at = datetime.now()
    
    rules[rule_id] = rule
    save_rules(rules)
    
    return rule

@app.post("/rules/{rule_id}/submit", response_model=Rule, summary="Submit rule for approval", tags=["Rule Approval"])
async def submit_for_approval(rule_id: str):
    """
    Submit a draft rule for approval. Changes status from 'draft' to 'pending_approval'.
    """
    rules = load_rules()
    
    if rule_id not in rules:
        raise HTTPException(status_code=404, detail=f"Rule with ID '{rule_id}' not found")
    
    rule = rules[rule_id]
    
    if rule.status != RuleStatus.DRAFT:
        raise HTTPException(status_code=400, detail=f"Only draft rules can be submitted for approval. Current status: {rule.status}")
    
    rule.status = RuleStatus.PENDING_APPROVAL
    rule.updated_at = datetime.now()
    
    rules[rule_id] = rule
    save_rules(rules)
    
    return rule

@app.delete("/rules/{rule_id}", summary="Delete a rule", tags=["Rule Management"])
async def delete_rule(rule_id: str):
    """
    Delete a rule permanently. Use with caution!
    """
    rules = load_rules()
    
    if rule_id not in rules:
        raise HTTPException(status_code=404, detail=f"Rule with ID '{rule_id}' not found")
    
    deleted_rule = rules[rule_id]
    del rules[rule_id]
    save_rules(rules)
    
    return {"message": f"Rule '{rule_id}' deleted successfully", "deleted_rule": deleted_rule}

@app.get("/rules/stats/summary", summary="Get rules statistics", tags=["Statistics"])
async def get_rules_stats():
    """
    Get summary statistics about all rules in the system.
    """
    rules = load_rules()
    
    if not rules:
        return {
            "total_rules": 0,
            "status_breakdown": {},
            "tags_summary": {},
            "recent_activity": []
        }
    
    # Status breakdown
    status_counts = {}
    for rule in rules.values():
        status = rule.status.value
        status_counts[status] = status_counts.get(status, 0) + 1
    
    # Tags summary
    tags_counts = {}
    for rule in rules.values():
        for tag in rule.data.tags:
            tags_counts[tag] = tags_counts.get(tag, 0) + 1
    
    # Recent activity (last 5 updated rules)
    recent_rules = sorted(rules.values(), key=lambda x: x.updated_at, reverse=True)[:5]
    recent_activity = [
        {
            "rule_id": rule.id,
            "name": rule.data.name,
            "status": rule.status.value,
            "updated_at": rule.updated_at.isoformat()
        }
        for rule in recent_rules
    ]
    
    return {
        "total_rules": len(rules),
        "status_breakdown": status_counts,
        "tags_summary": tags_counts,
        "recent_activity": recent_activity
    }

# Run the application
if __name__ == "__main__":
    import uvicorn
    print("Starting Rules Management API...")
    print("Swagger UI will be available at: http://localhost:8000/docs")
    print("ReDoc documentation at: http://localhost:8000/redoc")
    uvicorn.run(app, host="0.0.0.0", port=8000)