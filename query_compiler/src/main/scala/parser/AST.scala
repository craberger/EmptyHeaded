package DunceCap

abstract trait ASTStatement {}
abstract trait ASTConvergenceCondition {}

//input to this should be 
//(1) list of attrs in the output, 
//(2) list of attrs eliminated (aggregations), 
//(3) list of relations joined
//(4) list of attrs with selections
//(5) list of exressions for aggregations
class ASTLambdaFunction(val inputArgument:QueryRelation,
                        val join:List[QueryRelation],
                        val aggregates:Map[String,ParsedAggregate])


case class ASTItersCondition(iters:Int) extends ASTConvergenceCondition
case class ASTEpsilonCondition(eps:Double) extends ASTConvergenceCondition

case class ASTQueryStatement(lhs:QueryRelation,
                             convergence:Option[ASTConvergenceCondition],
                             joinType:String,
                             join:List[QueryRelation],
                             joinAggregates:Map[String,ParsedAggregate]) extends ASTStatement {
  // TODO (sctu) : ignoring everything except for join, joinAggregates for now
  def dependsOn(statement: ASTQueryStatement): Boolean = {
    val namesInStatement = (statement.join.map(rels => rels.name):::statement.joinAggregates.values.map(parsedAgg => parsedAgg.expression).toList).toSet
    join.find(rel => namesInStatement.contains(rel.name)).isDefined
  }

  def computePlan(config:Config, isRecursive:Boolean): QueryPlan = {
    val annotationSetSuccess = join.map(rel => Environment.setAnnotationAccordingToConfig(rel))
    if (annotationSetSuccess.find(b => !b).isDefined) {
      throw new RelationNotFoundException("TODO: fill in with a better explanation")
    }

    if (!config.nprrOnly) {
      val rootNodes = GHDSolver.getMinFHWDecompositions(join);
      val candidates = rootNodes.map(r => new GHD(r, join, joinAggregates, lhs));
      candidates.map(c => c.doPostProcessingPass())
      val chosen = HeuristicUtils.getGHDsWithMaxCoveringRoot(
        HeuristicUtils.getGHDsOfMinHeight(HeuristicUtils.getGHDsWithMinBags(candidates)))
      if (config.bagDedup) {
        chosen.head.doBagDedup
      }
      chosen.head.getQueryPlan
    } else {
      // since we're only using NPRR, just create a single GHD bag
      val oneBag = new GHD(new GHDNode(join) ,join, joinAggregates, lhs)
      oneBag.doPostProcessingPass
      oneBag.getQueryPlan
    }
  }
}
