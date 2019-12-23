// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"fmt"
	"strings"

	"github.com/hanchuanchuan/goInception/ast"
	"github.com/hanchuanchuan/goInception/bindinfo"
	"github.com/hanchuanchuan/goInception/config"
	"github.com/hanchuanchuan/goInception/domain"
	"github.com/hanchuanchuan/goInception/infoschema"
	"github.com/hanchuanchuan/goInception/metrics"
	"github.com/hanchuanchuan/goInception/parser"
	"github.com/hanchuanchuan/goInception/planner"
	plannercore "github.com/hanchuanchuan/goInception/planner/core"
	"github.com/hanchuanchuan/goInception/sessionctx"
	"github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// Compiler compiles an ast.StmtNode to a physical plan.
type Compiler struct {
	Ctx sessionctx.Context
}

// Compile compiles an ast.StmtNode to a physical plan.
func (c *Compiler) Compile(ctx context.Context, stmtNode ast.StmtNode) (*ExecStmt, error) {
	return c.compile(ctx, stmtNode, false)
}
func (c *Compiler) compile(ctx context.Context, stmtNode ast.StmtNode, skipBind bool) (*ExecStmt, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span1 := opentracing.StartSpan("executor.Compile", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	if !skipBind {
		stmtNode = addHint(c.Ctx, stmtNode)
	}

	infoSchema := GetInfoSchema(c.Ctx)
	if err := plannercore.Preprocess(c.Ctx, stmtNode, infoSchema); err != nil {
		return nil, err
	}

	finalPlan, err := planner.Optimize(ctx, c.Ctx, stmtNode, infoSchema)
	if err != nil {
		return nil, err
	}

	CountStmtNode(stmtNode, c.Ctx.GetSessionVars().InRestrictedSQL)
	isExpensive := logExpensiveQuery(stmtNode, finalPlan)

	return &ExecStmt{
		InfoSchema: infoSchema,
		Plan:       finalPlan,
		Expensive:  isExpensive,
		Cacheable:  plannercore.Cacheable(stmtNode),
		Text:       stmtNode.Text(),
		StmtNode:   stmtNode,
		Ctx:        c.Ctx,
	}, nil
}

func logExpensiveQuery(stmtNode ast.StmtNode, finalPlan plannercore.Plan) (expensive bool) {
	expensive = isExpensiveQuery(finalPlan)
	if !expensive {
		return
	}

	const logSQLLen = 1024
	sql := stmtNode.Text()
	if len(sql) > logSQLLen {
		sql = fmt.Sprintf("%s len(%d)", sql[:logSQLLen], len(sql))
	}
	log.Warnf("[EXPENSIVE_QUERY] %s", sql)
	return
}

func isExpensiveQuery(p plannercore.Plan) bool {
	switch x := p.(type) {
	case plannercore.PhysicalPlan:
		return isPhysicalPlanExpensive(x)
	case *plannercore.Execute:
		return isExpensiveQuery(x.Plan)
	case *plannercore.Insert:
		if x.SelectPlan != nil {
			return isPhysicalPlanExpensive(x.SelectPlan)
		}
	case *plannercore.Delete:
		if x.SelectPlan != nil {
			return isPhysicalPlanExpensive(x.SelectPlan)
		}
	case *plannercore.Update:
		if x.SelectPlan != nil {
			return isPhysicalPlanExpensive(x.SelectPlan)
		}
	}
	return false
}

func isPhysicalPlanExpensive(p plannercore.PhysicalPlan) bool {
	expensiveRowThreshold := int64(config.GetGlobalConfig().Log.ExpensiveThreshold)
	if int64(p.StatsCount()) > expensiveRowThreshold {
		return true
	}

	for _, child := range p.Children() {
		if isPhysicalPlanExpensive(child) {
			return true
		}
	}

	return false
}

// CountStmtNode records the number of statements with the same type.
func CountStmtNode(stmtNode ast.StmtNode, inRestrictedSQL bool) {
	if inRestrictedSQL {
		return
	}
}

// GetStmtLabel generates a label for a statement.
func GetStmtLabel(stmtNode ast.StmtNode) string {
	switch x := stmtNode.(type) {
	case *ast.AlterTableStmt:
		return "AlterTable"
	case *ast.AnalyzeTableStmt:
		return "AnalyzeTable"
	case *ast.BeginStmt:
		return "Begin"
	case *ast.CommitStmt:
		return "Commit"
	case *ast.CreateDatabaseStmt:
		return "CreateDatabase"
	case *ast.CreateIndexStmt:
		return "CreateIndex"
	case *ast.CreateTableStmt:
		return "CreateTable"
	case *ast.CreateUserStmt:
		return "CreateUser"
	case *ast.DeleteStmt:
		return "Delete"
	case *ast.DropDatabaseStmt:
		return "DropDatabase"
	case *ast.DropIndexStmt:
		return "DropIndex"
	case *ast.DropTableStmt:
		return "DropTable"
	case *ast.ExplainStmt:
		return "Explain"
	case *ast.InsertStmt:
		if x.IsReplace {
			return "Replace"
		}
		return "Insert"
	case *ast.LoadDataStmt:
		return "LoadData"
	case *ast.RollbackStmt:
		return "RollBack"
	case *ast.SelectStmt:
		return "Select"
	case *ast.SetStmt, *ast.SetPwdStmt:
		return "Set"
	case *ast.ShowStmt:
		return "Show"
	case *ast.TruncateTableStmt:
		return "TruncateTable"
	case *ast.UpdateStmt:
		return "Update"
	case *ast.GrantStmt:
		return "Grant"
	case *ast.RevokeStmt:
		return "Revoke"
	case *ast.DeallocateStmt:
		return "Deallocate"
	case *ast.ExecuteStmt:
		return "Execute"
	case *ast.PrepareStmt:
		return "Prepare"
	case *ast.UseStmt:
		return "Use"
	}
	return "other"
}

// GetInfoSchema gets TxnCtx InfoSchema if snapshot schema is not set,
// Otherwise, snapshot schema is returned.
func GetInfoSchema(ctx sessionctx.Context) infoschema.InfoSchema {
	sessVar := ctx.GetSessionVars()
	var is infoschema.InfoSchema
	if snap := sessVar.SnapshotInfoschema; snap != nil {
		is = snap.(infoschema.InfoSchema)
		log.Infof("con:%d use snapshot schema %d", sessVar.ConnectionID, is.SchemaMetaVersion())
	} else {
		is = sessVar.TxnCtx.InfoSchema.(infoschema.InfoSchema)
	}
	return is
}

func addHint(ctx sessionctx.Context, stmtNode ast.StmtNode) ast.StmtNode {
	if ctx.Value(bindinfo.SessionBindInfoKeyType) == nil { //when the domain is initializing, the bind will be nil.
		return stmtNode
	}
	switch x := stmtNode.(type) {
	case *ast.ExplainStmt:
		switch x.Stmt.(type) {
		case *ast.SelectStmt:
			normalizeExplainSQL := parser.Normalize(x.Text())
			idx := strings.Index(normalizeExplainSQL, "select")
			normalizeSQL := normalizeExplainSQL[idx:]
			hash := parser.DigestHash(normalizeSQL)
			x.Stmt = addHintForSelect(hash, normalizeSQL, ctx, x.Stmt)
		}
		return x
	case *ast.SelectStmt:
		normalizeSQL, hash := parser.NormalizeDigest(x.Text())
		return addHintForSelect(hash, normalizeSQL, ctx, x)
	default:
		return stmtNode
	}
}

func addHintForSelect(hash, normdOrigSQL string, ctx sessionctx.Context, stmt ast.StmtNode) ast.StmtNode {
	sessionHandle := ctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindRecord := sessionHandle.GetBindRecord(normdOrigSQL, ctx.GetSessionVars().CurrentDB)
	if bindRecord != nil {
		if bindRecord.Status == bindinfo.Invalid {
			return stmt
		}
		if bindRecord.Status == bindinfo.Using {
			metrics.BindUsageCounter.WithLabelValues(metrics.ScopeSession).Inc()
			return bindinfo.BindHint(stmt, bindRecord.Ast)
		}
	}
	globalHandle := domain.GetDomain(ctx).BindHandle()
	bindRecord = globalHandle.GetBindRecord(hash, normdOrigSQL, ctx.GetSessionVars().CurrentDB)
	if bindRecord == nil {
		bindRecord = globalHandle.GetBindRecord(hash, normdOrigSQL, "")
	}
	if bindRecord != nil {
		metrics.BindUsageCounter.WithLabelValues(metrics.ScopeGlobal).Inc()
		return bindinfo.BindHint(stmt, bindRecord.Ast)
	}
	return stmt
}
