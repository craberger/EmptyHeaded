package solver

import DunceCap.{Config, ASTQueryStatement}

object EvalGraph {
  def isRecursive(statement: ASTQueryStatement) = {
    statement.dependsOn(statement)
  }

  def computeDependencyGraph(statements: Set[ASTQueryStatement]): EvalGraphNode = {
    // one ASTStatement could be dependent on another if it has the other's output
    // in it's list of input rels, or in its annotation
    val start = EvalGraphNode(statements.head, EvalGraph.isRecursive(statements.head))
    val (deps, notDeps) = statements.tail.partition(statement => statements.head.dependsOn(statement))
    val notDepsSet = notDeps.toSet
    val depsSet = deps.toSet
    start.deps = deps.flatMap(dep => {
      val leftOverStatements = (deps-dep)++notDepsSet
      if (!leftOverStatements.isEmpty) {
        Some(computeDependencyGraph(leftOverStatements))
      } else {
        None
      }
    })
    return start
  }
}
case class EvalGraph(val statements:List[ASTQueryStatement]) {
  val dependencyGraph = EvalGraph.computeDependencyGraph(statements.toSet)
  def computePlan(config:Config): List[QueryPlan] = {
    dependencyGraph.computePlan(config).reverse
  }
}

case class EvalGraphNode(val statement:ASTQueryStatement, val recursive:Boolean) {
  var deps: Set[EvalGraphNode] = null
  def computePlan(config:Config): List[QueryPlan] = {
    statement.computePlan(config, recursive)::deps.toList.flatMap(_.computePlan(config))
  }
}
