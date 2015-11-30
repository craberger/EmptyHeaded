package solver

import DunceCap._

object EvalGraph {
  def isRecursive(statement:ASTQueryStatement) = {
    statement.dependsOn(statement)
  }

  def computeDependencyGraph(statements: List[ASTQueryStatement]): EvalGraphNode = {
    // one ASTStatement could be dependent on another if it has the other's output
    // in it's list of input rels, or in its annotation
    val (start, left) = if (EvalGraph.isRecursive(statements.head)) {
      val otherPart = statements.tail.find(st => st.lhs.name == statements.head.lhs.name)
      if(otherPart.isEmpty) {
        throw InfiniteRecursionException("")
      }
      (EvalGraphNode(statements.head, otherPart), statements.tail.filter(st => st.lhs.name != statements.head.lhs.name))
    } else {
      (EvalGraphNode(statements.head, None), statements.tail)
    }

    val (deps, notDeps) = left.partition(statement => start.dependsOn(statement))
    println(deps)
    println(notDeps)
    val notDepsSet = notDeps.toSet
    val depsSet = deps.toSet
    start.deps = deps.flatMap(dep => {
      /* hack to get this to work for now, obviously not general */
      Some(EvalGraphNode(dep, None))
    })
    return start
  }
}

case class EvalGraph(val statements:List[ASTQueryStatement]) {
  val dependencyGraph = EvalGraph.computeDependencyGraph(statements.reverse)
  def computePlan(config:Config): List[QueryPlan] = {
    println(dependencyGraph)
    dependencyGraph.computePlan(config).reverse
  }
}

case class EvalGraphNode(val statement:ASTQueryStatement, val baseCase:Option[ASTQueryStatement]) {
  var deps: List[EvalGraphNode] = List()
  def computePlan(config:Config): List[QueryPlan] = {
    println(statement.lhs.name)
    val thisPlan = if (baseCase.isDefined) {
      val root = new GHDNode(statement.join, statement.convergence)
      root.lhs = Some(statement.lhs)
      root.children = List(new GHDNode(baseCase.get.join))
      root.children.head.lhs = Some(baseCase.get.lhs)
      val ghd = new GHD(
        root,
        (statement.join++baseCase.get.join), statement.joinAggregates++baseCase.get.joinAggregates,
        QueryRelation(statement.lhs.name, statement.lhs.attrs:::baseCase.get.lhs.attrs, statement.lhs.annotationType))
      ghd.doPostProcessingPass()
      ghd.getQueryPlan(statement.convergence)
    } else {
      statement.computePlan(config, false)
    }
    thisPlan::deps.flatMap(_.computePlan(config)).toList
  }

  def dependsOn(other:ASTQueryStatement) = {
    statement.dependsOn(other) || (baseCase.isDefined && baseCase.get.dependsOn(other))
  }
}
