// Copyright 2018 PingCAP, Inc.
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

package planner

import (
	"context"
	"github.com/hanchuanchuan/goInception/ast"
	"github.com/hanchuanchuan/goInception/infoschema"
	plannercore "github.com/hanchuanchuan/goInception/planner/core"
	"github.com/hanchuanchuan/goInception/privilege"
	"github.com/hanchuanchuan/goInception/sessionctx"
	"github.com/pingcap/errors"
)

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, error) {
	fp := plannercore.TryFastPlan(sctx, node)
	if fp != nil {
		return fp, nil
	}

	// build logical plan
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	builder := plannercore.NewPlanBuilder(sctx, is)
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, err
	}

	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the table information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if !plannercore.CheckPrivilege(pm, builder.GetVisitInfo()) {
			return nil, errors.New("privilege check fail")
		}
	}

	// Handle the execute statement.
	if execPlan, ok := p.(*plannercore.Execute); ok {
		err := execPlan.OptimizePreparedPlan(ctx, sctx, is)
		return p, err
	}

	// Handle the non-logical plan statement.
	logic, isLogicalPlan := p.(plannercore.LogicalPlan)
	if !isLogicalPlan {
		return p, nil
	}

	// Handle the logical plan statement, use cascades planner if enabled.
	if sctx.GetSessionVars().EnableCascadesPlanner {
		return nil, errors.New("the cascades planner is not implemented yet")
	}
	return plannercore.DoOptimize(builder.GetOptFlag(), logic)
}

func init() {
	plannercore.OptimizeAstNode = Optimize
}
