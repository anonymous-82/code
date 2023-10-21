/*-------------------------------------------------------------------------
 *
 * nodeNestloop.c
 *	  routines to support nest-loop joins
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeNestloop.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecNestLoop	 - process a nestloop join of two plans
 *		ExecInitNestLoop - initialize the join
 *		ExecEndNestLoop  - shut down the join
 */

#include "postgres.h"

#include <math.h>
#include <stdlib.h>

#include "executor/execdebug.h"
#include "executor/nodeNestloop.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "fmgr.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"


/* ----------------------------------------------------------------
 *		ExecNestLoop(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer * relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */

#define MAX(a,b) ((a) > (b) ? (a) : (b))

static RelationPage* CreateRelationPage() {
	int i;
	RelationPage *relationPage = palloc(sizeof(RelationPage));
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	for (i = 0; i < PAGE_SIZE; i++) {
		relationPage->tuples[i] = NULL;
	}
	return relationPage;
}

static void RemoveRelationPage(RelationPage **relationPageAdr) {
	int i;
	RelationPage *relationPage;
	relationPage = *relationPageAdr;
	if (relationPage == NULL) {
		return;
	}
	for (i = 0; i < PAGE_SIZE; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	pfree(relationPage);
	(*relationPageAdr) = NULL;
}

static int LoadNextPage(PlanState *planState, RelationPage *relationPage) {
	int i;
	if (relationPage == NULL) {
		elog(ERROR, "LoadNextPage: null page");
	}
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	for (i = 0; i < PAGE_SIZE; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	for (i = 0; i < PAGE_SIZE; i++) {
		TupleTableSlot *tts = ExecProcNode(planState);
		if (TupIsNull(tts)) {
			relationPage->tuples[i] = NULL;
			break;
		} else {
			relationPage->tuples[i] = MakeSingleTupleTableSlot(tts->tts_tupleDescriptor);
			ExecCopySlot(relationPage->tuples[i], tts);
			relationPage->tupleCount++;
		}
	}
	return relationPage->tupleCount;
}

static int LoadNextOuterPage(PlanState *outerPlan, RelationPage *relationPage, ScanKey xidScanKey, int fromPageIndex) {
	int i;
	TupleTableSlot *tts;
	int fromXid;
	fromXid = fromPageIndex * PAGE_SIZE + 1;
	if (relationPage == NULL) {
		elog(ERROR, "LoadNextOuterPage: null page");
	}
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	for (i = 0; i < PAGE_SIZE; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	for (i = 0; i < PAGE_SIZE; i++) {
		ScanKeyEntryInitialize(xidScanKey,
				0,
				1,
				3,
				23,
				0,
				65,
				fromXid + i);
		if (IsA(outerPlan, IndexScanState)) {
			((IndexScanState*) outerPlan)->iss_NumScanKeys = 1;
			((IndexScanState*) outerPlan)->iss_ScanKeys = xidScanKey;
		} else if (IsA(outerPlan, IndexOnlyScanState)) {
			((IndexOnlyScanState*) outerPlan)->ioss_NumScanKeys = 1;
			((IndexOnlyScanState*) outerPlan)->ioss_ScanKeys = xidScanKey;
		} else {
			elog(ERROR, "Outer plan type is not index scan");
		}
		ExecReScan(outerPlan);
		tts = ExecProcNode(outerPlan);
		if (TupIsNull(tts)) {
			relationPage->tuples[i] = NULL;
			break;
		} else {
			relationPage->tuples[i] = MakeSingleTupleTableSlot(tts->tts_tupleDescriptor);
			ExecCopySlot(relationPage->tuples[i], tts);
			relationPage->tupleCount++;
		}
	}
	return relationPage->tupleCount;
}

static void LoadPageWithTIDs(PlanState *outerPlan, struct tupleRewards *tids, RelationPage *relationPage, int index,
		Relation heapRelation, TupleTableSlot *tup) {
	Buffer tempBuf = InvalidBuffer;
	int i = 0;
	TupleTableSlot *tts = tup;
	if (relationPage == NULL) {
		elog(ERROR, "LoadNextOuterPage: null page");
	}
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	int size = tids[index].size;
	for (i = 0; i < size; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}

	for (i = 0; i < size; i++) {
		while (true) {
			if (heap_fetch(heapRelation, outerPlan->state->es_snapshot, &tids[index].tuples[i], &tempBuf, false,
					NULL)) {
				ExecStoreTuple(&tids[index].tuples[i], tts, tempBuf,
				false);
				if (TupIsNull(tts)) {
					ReleaseBuffer(tempBuf);
					continue;
				} else {
					relationPage->tuples[i] = MakeSingleTupleTableSlot(tts->tts_tupleDescriptor);
					ExecCopySlot(relationPage->tuples[i], tts);
					relationPage->tupleCount++;
					ReleaseBuffer(tempBuf);
				}
				break;
			}
		}
	}
}

static int popBestPageXid(NestLoopState *node) {
	int i;
	int bestPageIndex;
	int bestXid;

	bestPageIndex = 0;
	for (i = 0; i < node->activeRelationPages; i++) {
		if (node->rewards[i] > node->rewards[bestPageIndex]) {
			bestPageIndex = i;
		}
	}
	elog(INFO, "reward: %d", node->rewards[bestPageIndex]);
	bestXid = node->xids[bestPageIndex];
	node->xids[bestPageIndex] = node->xids[node->activeRelationPages - 1];
	node->rewards[bestPageIndex] = node->rewards[node->activeRelationPages - 1];
	node->activeRelationPages--;
	return bestXid;
}

static int popBestPage(NestLoopState *node) {
	int i;
	int bestPageIndex = 0;
	
	double rewardsTotal = 0.0;
	double randomNum = 0.0;
	double cumulative_prob = 0.0;
	double randomValue = 0.0;
	
	for (i = 0; i < node->activeRelationPages; i++) {
		node->idxreward[i].rwrd = node->tidRewards[i].reward + 1;
		rewardsTotal = rewardsTotal + node->idxreward[i].rwrd;
		node->idxreward[i].orig_idx = i;
	}

	rewardsTotal = rewardsTotal + (1 * node->remOuterPages);
	for (i = 0; i < node->activeRelationPages; i++) {
		node->idxreward[i].rwrdratio = (double) node->idxreward[i].rwrd / rewardsTotal;
	}
	
	int compareIndexedRewards(const void* a, const void* b) {
		double diff = ((struct indexedReward*)a)->rwrd - ((struct indexedReward*)b)->rwrd;
		if (diff < 0) return -1;
		if (diff > 0) return 1;
		return 0;
	}
	qsort(node->idxreward, node->activeRelationPages, sizeof(struct indexedReward), compareIndexedRewards);
	
	srand(time(NULL));
	randomValue =((double) rand() / RAND_MAX);
	randomNum = (double)(randomValue * rewardsTotal);

	for (i = 0; i < node->activeRelationPages; i++) {
		cumulative_prob += node->idxreward[i].rwrd;
		bestPageIndex = node->idxreward[i].orig_idx;
		if (randomNum <= cumulative_prob) {
			break;
		}
	}

	node->pageNum = node->tidRewards[bestPageIndex].outpgnum;
	node->reRatio = node->idxreward[bestPageIndex].rwrdratio;
	node->tidRewards[bestPageIndex].rewardRatio = -1.0;
	node->tidRewards[bestPageIndex].reward = -1;
	return bestPageIndex;
}

static void storeTIDs(RelationPage *relationPage, struct tupleRewards *tids, int index, int reward, int outerpagenum) {
	int i = 0;
	tids[index].reward = reward;
	tids[index].size = relationPage->tupleCount;
	tids[index].outpgnum = outerpagenum - 1;

	for (i = 0; i < relationPage->tupleCount; i++) {
		tids[index].tuples[i] = *relationPage->tuples[i]->tts_tuple;
	}
}

static void PrintNodeCounters(NestLoopState *node) {
	elog(INFO, "Read outer pages: %d", node->outerPageCounter);
	elog(INFO, "Read inner pages: %d", node->innerPageCounterTotal);
	elog(INFO, "Read outer tuples: %ld", node->outerTupleCounter);
	elog(INFO, "Read inner tuples: %ld", node->innerTupleCounter);
	elog(INFO, "Generated joins: %d", node->generatedJoins);
	elog(INFO, "Exploit Generated joins: %d", node->genExploit);
	elog(INFO, "Explore Generated joins: %d", node->genExplore);
	elog(INFO, "Rescan Count: %d", node->rescanCount);
	elog(INFO, "Current XidPage: %d", node->pageIndex);
	elog(INFO, "Active Relations: %d", node->activeRelationPages);
	elog(INFO, "Total page reads: %d", (node->outerPageCounter + node->innerPageCounterTotal));
}

static void calculateConfidenceInterval(NestLoopState *node) {
	double totalmean = 0.0;
	double temp_mean = 0.0;
	double variance = 0.0;
    double std_deviation = 0.0;
    double half_width = 0.0;
	double lower_bound = 0.0;
	double upper_bound = 0.0;
	double z_score = 0.0;
	int i = 0;
	int j = 0;
	double totalvariance = 0.0;
	double temp_variance = 0.0;
	double tuple_variance_numr = 0.0;
	double temp_var_num_explore = 0.0;
	double temp_var_num_exploit = 0.0;
	double difference_explr = 0.0;
	double difference_explt = 0.0;
	
	for (i = 0; i < (node->numOuterPages); i++) {
		if (node->outerpages[i].explore_page_reward_ratio != 0.0){
			for (j = 0; j < PAGE_SIZE; j++) {
				node->outerpages[i].outertupnum[j].outertupleinfo->explore_reward_ratio = (double) node->outerpages[i].explore_page_reward_ratio / PAGE_SIZE;
			}
		}
	}
	
	for (i = 0; i < (node->numOuterPages); i++) {
		for (j = 0; j < PAGE_SIZE; j++) {
			
			if (node->outerpages[i].outertupnum[j].outertupleinfo->explore_reward_ratio != 0.0) {
				node->outerpages[i].outertupnum[j].outertupleinfo->tuple_mean = (double) node->outerpages[i].outertupnum[j].outertupleinfo->explore_success_count / node->outerpages[i].outertupnum[j].outertupleinfo->explore_reward_ratio;
			}
			
			if (node->outerpages[i].outertupnum[j].outertupleinfo->exploit_reward_ratio != 0.0) {
				node->outerpages[i].outertupnum[j].outertupleinfo->tuple_mean += (double) node->outerpages[i].outertupnum[j].outertupleinfo->exploit_success_count / node->outerpages[i].outertupnum[j].outertupleinfo->exploit_reward_ratio;
			}
		}
	}
	
	for (i = 0; i < (node->numOuterPages); i++) {
		for (j = 0; j < PAGE_SIZE; j++) {
			temp_mean += (double) ( node->outerpages[i].outertupnum[j].outertupleinfo->explore_num_trails + node->outerpages[i].outertupnum[j].outertupleinfo->exploit_num_trails ) * node->outerpages[i].outertupnum[j].outertupleinfo->tuple_mean;
		}
	}
	
	totalmean = (double) temp_mean / node->T;
	temp_mean = 0.0;

	for (i = 0; i < (node->numOuterPages); i++) {
		for (j = 0; j < PAGE_SIZE; j++) {
			if (node->outerpages[i].outertupnum[j].outertupleinfo->explore_reward_ratio != 0.0) {
				difference_explr = ((double) node->outerpages[i].outertupnum[j].outertupleinfo->explore_num_trails * node->outerpages[i].outertupnum[j].outertupleinfo->tuple_mean) - totalmean;
				temp_var_num_explore = difference_explr * difference_explr;
			}
			if (node->outerpages[i].outertupnum[j].outertupleinfo->exploit_reward_ratio != 0.0) {
				difference_explt = ((double) node->outerpages[i].outertupnum[j].outertupleinfo->exploit_num_trails * node->outerpages[i].outertupnum[j].outertupleinfo->tuple_mean) - totalmean;
				temp_var_num_exploit = difference_explt * difference_explt;
			}

			tuple_variance_numr = (double) temp_var_num_explore + temp_var_num_exploit;
			
			node->outerpages[i].outertupnum[j].outertupleinfo->tuple_variance = (double) tuple_variance_numr;
			temp_var_num_explore = 0.0;
			temp_var_num_exploit = 0.0;
			tuple_variance_numr = 0.0;
			difference_explr = 0.0;
			difference_explt = 0.0;
		}
	}

	for (i = 0; i < (node->numOuterPages); i++) {
		for (j = 0; j < PAGE_SIZE; j++) {
			temp_variance += (double) node->outerpages[i].outertupnum[j].outertupleinfo->tuple_variance;
		}
	}

	totalvariance = (double) temp_variance / node->T;
	temp_variance = 0.0;
	std_deviation = sqrt(totalvariance);
	
    z_score = 1.96;
	half_width = z_score * std_deviation / sqrt(node->T);

    lower_bound = totalmean - half_width;
    upper_bound = totalmean + half_width;
	
    elog(INFO, "Randomized m-run Estimator: Confidence Interval is	%f	<	%f	<	%f", lower_bound, totalmean, upper_bound);
	}

static TupleTableSlot* ExecRightBanditJoin(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;
	ListCell *lc;

	CHECK_FOR_INTERRUPTS();

	ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = innerPlanState(node);
	innerPlan = outerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	ResetExprContext(econtext);

	ENL1_printf("entering main loop");

	if (node->innerTupleCounter == 0)
		ExecReScan(outerPlan);

	for (;;) {
		if (node->needOuterPage) {
			if (!node->reachedEndOfOuter && node->activeRelationPages < node->sqrtOfInnerPages) {
				node->isExploring = true;
				node->pageIndex++;
				node->pageIndex = MAX(node->pageIndex, node->lastPageIndex);
				LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, node->pageIndex);
				if (node->outerPage->tupleCount < PAGE_SIZE) {
					elog(INFO, "Reached end of outer");
					node->reachedEndOfOuter = true;
					if (node->outerPage->tupleCount == 0)
						continue;
				}
				node->outerTupleCounter += node->outerPage->tupleCount;
				node->outerPageCounter++;
				node->lastReward = 0;
				node->exploreStepCounter = 1;
			} else if ((!node->reachedEndOfOuter && node->activeRelationPages == node->sqrtOfInnerPages)
					|| (node->reachedEndOfOuter && node->activeRelationPages > 0)) {
				node->outerPage->index = 0;
				node->isExploring = false;
				node->exploitStepCounter = 0;
				node->lastPageIndex = MAX(node->pageIndex, node->lastPageIndex);
				node->pageIndex = popBestPageXid(node);
				LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, node->pageIndex);
			} else {
				elog(INFO, "Join finished normally");
				return NULL;

			}
			node->needOuterPage = false;
			node->needInnerPage = true;
		}
		if (node->needInnerPage) {
			if (node->reachedEndOfInner) {
				foreach(lc, nl->nestParams)
				{
					NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
					int paramno = nlp->paramno;
					ParamExecData *prm;

					prm = &(econtext->ecxt_param_exec_vals[paramno]);
					Assert(IsA(nlp->paramval, Var));
					Assert(nlp->paramval->varno == OUTER_VAR);
					Assert(nlp->paramval->varattno > 0);
					prm->value = slot_getattr(node->outerPage->tuples[0], nlp->paramval->varattno, &(prm->isnull));
					innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
				}
				node->innerPageCounter = 0;
				ExecReScan(innerPlan);
				node->rescanCount++;
				node->reachedEndOfInner = false;
			}
			LoadNextPage(innerPlan, node->innerPage);
			if (node->innerPage->tupleCount < PAGE_SIZE) {
				node->reachedEndOfInner = true;
				if (node->innerPage->tupleCount == 0)
					continue;
			}
			node->innerTupleCounter += node->innerPage->tupleCount;
			node->innerPageCounter++;
			node->innerPageCounterTotal++;
			node->needInnerPage = false;
		}
		if (node->innerPage->index == node->innerPage->tupleCount) {
			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else {
				node->needInnerPage = true;
				if (node->isExploring && node->lastReward > 0 && node->exploreStepCounter < node->innerPageNumber) {
					node->outerPage->index = 0;
					node->reward += node->lastReward;
					node->lastReward = 0;
					node->exploreStepCounter++;
				} else if (node->isExploring && node->exploreStepCounter == node->innerPageNumber) {
					node->needOuterPage = true;
				} else if (node->isExploring && node->lastReward == 0) {
					node->xids[node->activeRelationPages] = node->pageIndex;
					node->rewards[node->activeRelationPages] = node->reward;
					node->activeRelationPages++;
					node->needOuterPage = true;
				} else if (!node->isExploring && node->exploitStepCounter < node->innerPageNumber) {
					node->outerPage->index = 0;
					node->exploitStepCounter++;
				} else if (!node->isExploring && node->exploitStepCounter == node->innerPageNumber) {
					node->needOuterPage = true;
				} else {
					elog(ERROR, "Error");
				}
				continue;
			}
		}

		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
		innerTupleSlot = node->innerPage->tuples[node->innerPage->index];
		econtext->ecxt_outertuple = innerTupleSlot;
		econtext->ecxt_innertuple = outerTupleSlot;
		node->innerPage->index++;
		if (TupIsNull(innerTupleSlot)) {
			elog(WARNING, "inner tuple is null");
			return NULL;
		}
		if (TupIsNull(outerTupleSlot)) {
			if (node->activeRelationPages > 0) {
				elog(INFO, "Null outer detected");
				node->needOuterPage = true;
				continue;
			}
			return NULL;
		}

		ENL1_printf("testing qualification");
		if (ExecQual(joinqual, econtext)) {

			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");
				node->lastReward++;
				node->generatedJoins++;
				if (node->pageIndex >= node->outerPageNumber) {
					elog(WARNING, "pageIndex > outerPageNumber!?");
					return NULL;
				}
				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);

		ResetExprContext(econtext); ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecBanditJoin(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;
	ListCell *lc;

	CHECK_FOR_INTERRUPTS();

	ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	node->ss = (ScanState*) outerPlan;

	ResetExprContext(econtext);

	ENL1_printf("entering main loop");

	for (;;) {
		if (node->needOuterPage) {
			if (!node->reachedEndOfOuter && node->activeRelationPages < node->sqrtOfInnerPages) {
				node->isExploring = true;
				node->pageIndex++;
				node->pageIndex = MAX(node->pageIndex, node->lastPageIndex);
				LoadNextPage(outerPlan, node->outerPage);
				if (node->outerPage->tupleCount < PAGE_SIZE) {
					elog(INFO, "Reached end of outer");
					node->reachedEndOfOuter = true;
					if (node->outerPage->tupleCount == 0)
						continue;
				}
				if (node->outerPageCounter == 0) {
					node->outerpages[node->outerPageCounter].explore_page_reward_ratio = (double) 1 / node->numOuterPages;
				}
				else {
					node->outerpages[node->outerPageCounter].explore_page_reward_ratio = (double) pow( (1 - node->outerpages[node->outerPageCounter-1].explore_p_r), node->outerpages[node->outerPageCounter-1].explore_n_value ) / (node->numOuterPages);
				}
				node->outerTupleCounter += node->outerPage->tupleCount;
				node->outerPageCounter++;
				node->lastReward = 0;
				node->exploreStepCounter = 1;
			} else if ((!node->reachedEndOfOuter && node->activeRelationPages == node->sqrtOfInnerPages)
					|| (node->reachedEndOfOuter && node->activeRelationPages > 0)) {
				node->outerPage->index = 0;
				node->isExploring = false;
				node->exploitStepCounter = 0;
				node->lastPageIndex = MAX(node->pageIndex, node->lastPageIndex);
				node->pageIndex = popBestPage(node);
				LoadPageWithTIDs(outerPlan, node->tidRewards, node->outerPage, node->pageIndex,
						node->ss->ss_currentRelation, node->ss->ss_ScanTupleSlot);
				node->tidRewards[node->pageIndex] = node->tidRewards[node->activeRelationPages - 1];
				node->activeRelationPages--;
			} else {
				calculateConfidenceInterval(node);
				elog(INFO, "Join finished normally");
				return NULL;

			}
			node->needOuterPage = false;
			node->needInnerPage = true;
		}
		if (node->needInnerPage) {
			if (node->reachedEndOfInner) {
				foreach(lc, nl->nestParams)
				{
					NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
					int paramno = nlp->paramno;
					ParamExecData *prm;

					prm = &(econtext->ecxt_param_exec_vals[paramno]);
					Assert(IsA(nlp->paramval, Var));
					Assert(nlp->paramval->varno == OUTER_VAR);
					Assert(nlp->paramval->varattno > 0);
					prm->value = slot_getattr(node->outerPage->tuples[0], nlp->paramval->varattno, &(prm->isnull));
					innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
				}
				node->innerPageCounter = 0;
				ExecReScan(innerPlan);
				node->rescanCount++;
				node->reachedEndOfInner = false;
			}

			LoadNextPage(innerPlan, node->innerPage);
			if (node->innerPage->tupleCount < PAGE_SIZE) {
				node->reachedEndOfInner = true;
				if (node->innerPage->tupleCount == 0)
					continue;
			}
			node->innerTupleCounter += node->innerPage->tupleCount;
			node->innerPageCounter++;
			node->innerPageCounterTotal++;
			node->needInnerPage = false;
		}
		if (node->innerPage->index == node->innerPage->tupleCount) {
			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else {
				node->needInnerPage = true;
				if (node->isExploring && ((node->lastReward > 0 && node->exploreStepCounter < node->innerPageNumber) 
										|| (node->lastReward == 0 && (++node->nFailure) < N_FAILURE))) {
					if (node->lastReward > 0) {
						node->pageSuccessCounter++;
						node->outerpages[node->outerPageCounter-1].explore_page_reward_ratio = (double) (1 - ( pow( (1 - (node->pageSuccessCounter / node->exploreStepCounter)), (node->nFailure - 1)) ));
					}
					node->outerPage->index = 0;
					node->reward += node->lastReward;
					node->lastReward = 0;
					node->exploreStepCounter++;
				} else if (node->isExploring && node->exploreStepCounter == node->innerPageNumber) {
					node->needOuterPage = true;
					node->outerpages[node->outerPageCounter-1].explore_n_value = node->nFailure - 1;
					node->outerpages[node->outerPageCounter-1].explore_p_r = (double) (node->exploreStepCounter - node->outerpages[node->outerPageCounter-1].explore_n_value) / node->exploreStepCounter;
					node->remOuterPages--;
					node->nFailure = 0;
					node->reward = 0;
				} else if (node->isExploring && node->lastReward == 0 && (++node->nFailure) >= N_FAILURE) {
					storeTIDs(node->outerPage, node->tidRewards, node->activeRelationPages, node->reward, node->outerPageCounter);
					node->outerpages[node->outerPageCounter-1].explore_n_value = node->nFailure - 1;
					node->outerpages[node->outerPageCounter-1].explore_p_r = (double) (node->exploreStepCounter - node->outerpages[node->outerPageCounter-1].explore_n_value) / node->exploreStepCounter;
					node->remOuterPages--;
					node->nFailure = 0;
					node->reward = 0;
					node->activeRelationPages++;
					node->needOuterPage = true;
				} else if (!node->isExploring && node->exploitStepCounter < node->innerPageNumber) {
					node->outerPage->index = 0;
					node->exploitStepCounter++;
				} else if (!node->isExploring && node->exploitStepCounter == node->innerPageNumber) {
					node->needOuterPage = true;
				} else {
					elog(ERROR, "Error");
				}
				continue;
			}
		}
		
		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
		econtext->ecxt_outertuple = outerTupleSlot;
		innerTupleSlot = node->innerPage->tuples[node->innerPage->index];
		econtext->ecxt_innertuple = innerTupleSlot;
		node->innerPage->index++;
		if (TupIsNull(innerTupleSlot)) {
			elog(WARNING, "inner tuple is null");
			return NULL;
		}
		if (TupIsNull(outerTupleSlot)) {
			if (node->activeRelationPages > 0) {
				elog(INFO, "Null outer detected");
				node->needOuterPage = true;
				continue;
			}
			return NULL;
		}

		ENL1_printf("testing qualification");
		if (!node->isExploring) {
			node->outerpages[node->pageNum].outertupnum[node->outerPage->index].outertupleinfo->exploit_num_trails++;
		}
		else {
			node->outerpages[node->outerPageCounter-1].outertupnum[node->outerPage->index].outertupleinfo->explore_num_trails++;
		}
		
		
		if (ExecQual(joinqual, econtext)) {
			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");
				node->lastReward++;
				node->generatedJoins++;
				if (!node->isExploring) {
					node->genExploit++;
					node->outerpages[node->pageNum].outertupnum[node->outerPage->index].outertupleinfo->exploit_success_count++;
					node->outerpages[node->pageNum].outertupnum[node->outerPage->index].outertupleinfo->exploit_reward_ratio = (double) node->reRatio / node->outerPage->tupleCount;
				}
				if (node->isExploring) {
					node->genExplore++;
					node->outerpages[node->outerPageCounter-1].outertupnum[node->outerPage->index].outertupleinfo->explore_success_count++;
				}
				if (node->pageIndex >= node->outerPageNumber) {
					elog(WARNING, "pageIndex > outerPageNumber!?");
					return NULL;
				}
				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);
		
		node->T = node->T + 1;
		if ( (node->generatedJoins%50 == 0) && (node->generatedJoins != node->currentCount) ) {
			elog(INFO, "node->generatedJoins is %d", node->generatedJoins);
			node->currentCount = node->generatedJoins;
			calculateConfidenceInterval(node);
		}
		ResetExprContext(econtext); ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecRightBlockNestedLoop(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;

	CHECK_FOR_INTERRUPTS(); ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = innerPlanState(node);
	innerPlan = outerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	ResetExprContext(econtext); ENL1_printf("entering main loop");

	if (nl->join.inner_unique)
		elog(WARNING, "inner relation is detected as unique");

	if (node->innerTupleCounter == 0)
		ExecReScan(outerPlan);

	for (;;) {
		if (node->needOuterPage) {
			if (node->reachedEndOfOuter) {
				RemoveRelationPage(&(node->outerPage));
				elog(INFO, "Join Done");
				return NULL;
			}
			node->outerPage = CreateRelationPage();
			LoadNextPage(outerPlan, node->outerPage);
			node->outerTupleCounter += node->outerPage->tupleCount;
			node->outerPageCounter++;
			node->needOuterPage = false;
			if (node->outerPage->tupleCount < PAGE_SIZE) {
				node->reachedEndOfOuter = true;
				if (node->outerPage->tupleCount == 0)
					continue;
			}
			ExecReScan(innerPlan);
			node->needInnerPage = true;
			node->rescanCount++;
		}
		if (node->needInnerPage) {
			LoadNextPage(innerPlan, node->innerPage);
			node->innerTupleCounter += node->innerPage->tupleCount;
			node->innerPageCounter++;
			node->innerPageCounterTotal++;
			node->needInnerPage = false;
		}
		if (node->innerPage->index == node->innerPage->tupleCount) {
			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else {
				if (node->innerPage->tupleCount < PAGE_SIZE) {
					node->needOuterPage = true;
				} else {
					node->outerPage->index = 0;
					node->needInnerPage = true;
				}
			}
			continue;
		}

		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
		innerTupleSlot = node->innerPage->tuples[node->innerPage->index];

		if (TupIsNull(innerTupleSlot)) {
			elog(ERROR, "Inner slot is null");
		}
		if (TupIsNull(outerTupleSlot)) {
			elog(INFO, "outer tuples: %d", node->outerPage->tupleCount);
			elog(INFO, "outer index: %d", node->outerPage->index);
			elog(INFO, "inner tuples: %d", node->innerPage->tupleCount);
			elog(INFO, "inner index: %d", node->innerPage->index);
			elog(INFO, "===============");
			PrintNodeCounters(node);
			elog(ERROR, "Outer slot is null");
		}
		econtext->ecxt_outertuple = innerTupleSlot;
		econtext->ecxt_innertuple = outerTupleSlot;
		node->innerPage->index++;

		ENL1_printf("testing qualification");

		if (ExecQual(joinqual, econtext)) {
			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");
				node->generatedJoins++;
				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);
		ResetExprContext(econtext); ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecBlockNestedLoop(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;
	ListCell *lc;

	CHECK_FOR_INTERRUPTS(); ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	ResetExprContext(econtext); ENL1_printf("entering main loop");

	if (nl->join.inner_unique)
		elog(WARNING, "inner relation is detected as unique");

	for (;;) {
		if (node->needOuterPage) {
			if (node->reachedEndOfOuter) {
				RemoveRelationPage(&(node->outerPage));
				elog(INFO, "Join Done");
				return NULL;
			}
			LoadNextPage(outerPlan, node->outerPage);
			node->outerTupleCounter += node->outerPage->tupleCount;
			node->outerPageCounter++;
			node->needOuterPage = false;
			if (node->outerPage->tupleCount < PAGE_SIZE) {
				node->reachedEndOfOuter = true;
				if (node->outerPage->tupleCount == 0)
					continue;
			}
		}
		if (node->needInnerPage) {
			LoadNextPage(innerPlan, node->innerPage);
			node->innerTupleCounter += node->innerPage->tupleCount;
			node->innerPageCounter++;
			node->innerPageCounterTotal++;
			node->needInnerPage = false;
			if (node->innerPage->tupleCount < PAGE_SIZE) {
				foreach(lc, nl->nestParams)
				{
					NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
					int paramno = nlp->paramno;
					ParamExecData *prm;

					prm = &(econtext->ecxt_param_exec_vals[paramno]);
					Assert(IsA(nlp->paramval, Var));
					Assert(nlp->paramval->varno == OUTER_VAR);
					Assert(nlp->paramval->varattno > 0);
					prm->value = slot_getattr(node->outerPage->tuples[node->outerPage->index], nlp->paramval->varattno,
							&(prm->isnull));
					innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
				} ENL1_printf("rescanning inner plan");
				ExecReScan(innerPlan);
				node->rescanCount++;
				node->needOuterPage = true;
				if (node->innerPage->tupleCount == 0) {
					node->needInnerPage = true;
					continue;
				}
			}
		}
		if (node->innerPage->index == node->innerPage->tupleCount) {
			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else {
				node->needInnerPage = true;
				node->outerPage->index = 0;
			}
			continue;
		}

		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
		innerTupleSlot = node->innerPage->tuples[node->innerPage->index];

		if (TupIsNull(innerTupleSlot)) {
			elog(ERROR, "Inner slot is null");
		}
		if (TupIsNull(outerTupleSlot)) {
			elog(INFO, "outer tuples: %d", node->outerPage->tupleCount);
			elog(INFO, "outer index: %d", node->outerPage->index);
			elog(INFO, "inner tuples: %d", node->innerPage->tupleCount);
			elog(INFO, "inner index: %d", node->innerPage->index);
			elog(INFO, "===============");
			PrintNodeCounters(node);
			elog(ERROR, "Outer slot is null");
		}
		econtext->ecxt_outertuple = outerTupleSlot;
		econtext->ecxt_innertuple = innerTupleSlot;
		node->innerPage->index++;

		ENL1_printf("testing qualification");

		if (ExecQual(joinqual, econtext)) {
			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");
				node->generatedJoins++;
				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);
		ResetExprContext(econtext); ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecRightRegularNestLoop(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;

	CHECK_FOR_INTERRUPTS();

	ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	ResetExprContext(econtext);

	ENL1_printf("entering main loop");

	if (node->innerTupleCounter == 0) {
		ExecReScan(innerPlan);
		innerTupleSlot = ExecProcNode(innerPlan);
		econtext->ecxt_innertuple = innerTupleSlot;
		if (TupIsNull(innerTupleSlot)) {
			return NULL;
		}
		node->innerTupleCounter++;
	}

	for (;;) {

		outerTupleSlot = ExecProcNode(outerPlan);
		if (TupIsNull(outerTupleSlot)) {
			ENL1_printf("no outer tuple, ending join");
			ExecReScan(outerPlan);
			innerTupleSlot = ExecProcNode(innerPlan);
			econtext->ecxt_innertuple = innerTupleSlot;
			if (TupIsNull(innerTupleSlot)) {
				return NULL;
			}
			node->innerTupleCounter++;
			continue;
		}
		node->outerTupleCounter++;
		ENL1_printf("saving new outer tuple information");
		econtext->ecxt_outertuple = outerTupleSlot;

		if (ExecQual(joinqual, econtext)) {
			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");

				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);

		ResetExprContext(econtext);

		ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecRegularNestLoop(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;
	ListCell *lc;

	CHECK_FOR_INTERRUPTS();

	ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	ResetExprContext(econtext);

	ENL1_printf("entering main loop");

	for (;;) {

		if (node->nl_NeedNewOuter) {
			ENL1_printf("getting new outer tuple");

			outerTupleSlot = ExecProcNode(outerPlan);
			node->outerTupleCounter++;

			if (TupIsNull(outerTupleSlot)) {
				ENL1_printf("no outer tuple, ending join");
				return NULL;
			}

			ENL1_printf("saving new outer tuple information");
			econtext->ecxt_outertuple = outerTupleSlot;
			node->nl_NeedNewOuter = false;
			node->nl_MatchedOuter = false;

			foreach(lc, nl->nestParams)
			{
				NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
				int paramno = nlp->paramno;
				ParamExecData *prm;

				prm = &(econtext->ecxt_param_exec_vals[paramno]);
				Assert(IsA(nlp->paramval, Var));
				Assert(nlp->paramval->varno == OUTER_VAR);
				Assert(nlp->paramval->varattno > 0);
				prm->value = slot_getattr(outerTupleSlot, nlp->paramval->varattno, &(prm->isnull));
				innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
			}

			ENL1_printf("rescanning inner plan");
			ExecReScan(innerPlan);
		}

		ENL1_printf("getting new inner tuple");

		innerTupleSlot = ExecProcNode(innerPlan);
		node->innerTupleCounter++;
		econtext->ecxt_innertuple = innerTupleSlot;

		if (TupIsNull(innerTupleSlot)) {
			ENL1_printf("no inner tuple, need new outer tuple");

			node->nl_NeedNewOuter = true;

			if (!node->nl_MatchedOuter && (node->js.jointype == JOIN_LEFT || node->js.jointype == JOIN_ANTI)) {
				econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

				ENL1_printf("testing qualification for outer-join tuple");

				if (otherqual == NULL || ExecQual(otherqual, econtext)) {
					ENL1_printf("qualification succeeded, projecting tuple");

					return ExecProject(node->js.ps.ps_ProjInfo);
				} else
					InstrCountFiltered2(node, 1);
			}

			continue;
		}

		ENL1_printf("testing qualification");

		if (ExecQual(joinqual, econtext)) {
			node->nl_MatchedOuter = true;

			if (node->js.jointype == JOIN_ANTI) {
				node->nl_NeedNewOuter = true;
				continue; /* return to top of loop */
			}

			if (node->js.single_match)
				node->nl_NeedNewOuter = true;

			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");

				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);

		ResetExprContext(econtext);

		ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecNestLoop(PlanState *pstate) {
	TupleTableSlot *tts;
	const char *fastjoin = GetConfigOption("enable_fastjoin", false, false);
	const char *blocknestloop = GetConfigOption("enable_block", false, false);
	const char *fliporder = GetConfigOption("enable_fliporder", false, false);
	if (strcmp(fastjoin, "on") == 0) {
		if (strcmp(fliporder, "on") == 0) {
			tts = ExecRightBanditJoin(pstate);
		} else {
			tts = ExecBanditJoin(pstate);
		}
	} else if (strcmp(blocknestloop, "on") == 0) {
		if (strcmp(fliporder, "on") == 0) {
			tts = ExecRightBlockNestedLoop(pstate);
		} else {
			tts = ExecBlockNestedLoop(pstate);
		}
	} else {
		if (strcmp(fliporder, "on") == 0) {
			tts = ExecRightRegularNestLoop(pstate);
		} else {
			tts = ExecRegularNestLoop(pstate);
		}
	}
	return tts;
}

/* ----------------------------------------------------------------
 *		ExecInitNestLoop
 * ----------------------------------------------------------------
 */
NestLoopState*
ExecInitNestLoop(NestLoop *node, EState *estate, int eflags) {
	NestLoopState *nlstate;
	int i;
	const char *fastjoin;
	const char *blocknestloop;
	const char *fliporder;

	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	NL1_printf("ExecInitNestLoop: %s\n",
			"initializing node");

	nlstate = makeNode(NestLoopState);
	nlstate->js.ps.plan = (Plan*) node;
	nlstate->js.ps.state = estate;
	nlstate->js.ps.ExecProcNode = ExecNestLoop;

	ExecAssignExprContext(estate, &nlstate->js.ps);

	outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
	if (node->nestParams == NIL)
		eflags |= EXEC_FLAG_REWIND;
	else
		eflags &= ~EXEC_FLAG_REWIND;
	innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);

	ExecInitResultTupleSlotTL(estate, &nlstate->js.ps);
	ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

	nlstate->js.ps.qual = ExecInitQual(node->join.plan.qual, (PlanState*) nlstate);
	nlstate->js.jointype = node->join.jointype;
	nlstate->js.joinqual = ExecInitQual(node->join.joinqual, (PlanState*) nlstate);

	nlstate->js.single_match = (node->join.inner_unique || node->join.jointype == JOIN_SEMI);

	switch (node->join.jointype) {
	case JOIN_INNER:
	case JOIN_SEMI:
		break;
	case JOIN_LEFT:
	case JOIN_ANTI:
		nlstate->nl_NullInnerTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(nlstate)));
		break;
	default:
		elog(ERROR, "unrecognized join type: %d",
		(int) node->join.jointype);
	}

	nlstate->nl_NeedNewOuter = true;
	nlstate->nl_MatchedOuter = false;

	fliporder = GetConfigOption("enable_fliporder", false, false);
	nlstate->activeRelationPages = 0;
	nlstate->isExploring = true;
	nlstate->lastReward = 0;
	nlstate->needOuterPage = true;
	nlstate->needInnerPage = true;
	nlstate->exploitStepCounter = 0;
	nlstate->innerPageCounter = 0;
	nlstate->innerPageCounterTotal = 0;
	nlstate->outerPageCounter = 0;
	nlstate->reachedEndOfOuter = false;
	nlstate->reachedEndOfInner = false;
	nlstate->innerTupleCounter = 0;
	nlstate->outerTupleCounter = 0;
	nlstate->generatedJoins = 0;
	nlstate->rescanCount = 0;
	nlstate->pageNum = 0;
	nlstate->nFailure = 0;
	nlstate->genExploit = 0;
	nlstate->genExplore = 0;
	nlstate->T = 0;
	nlstate->reRatio = 0.0;
	nlstate->numOuterTuples = 0;
	nlstate->numInnerTuples = 0;
	nlstate->currentCount = 0;
	nlstate->numOuterPages = 0;
	nlstate->remOuterPages = 0;
	nlstate->pageSuccessCounter = 0;
	nlstate->numInnerTuples = innerPlan(node)->plan_rows;
	nlstate->numOuterTuples = outerPlan(node)->plan_rows;

	double divisionResult = 0.0;
	if (strcmp(fliporder, "on") == 0) {
		nlstate->outerPageNumber = innerPlan(node)->plan_rows / PAGE_SIZE + 1;
		nlstate->innerPageNumber = outerPlan(node)->plan_rows / PAGE_SIZE + 1;
		divisionResult = (double) innerPlan(node)->plan_rows / PAGE_SIZE;
	} else {
		nlstate->outerPageNumber = outerPlan(node)->plan_rows / PAGE_SIZE + 1;
		nlstate->innerPageNumber = innerPlan(node)->plan_rows / PAGE_SIZE + 1;
		divisionResult = (double) outerPlan(node)->plan_rows / PAGE_SIZE;
	}
	
	if (divisionResult == (int) divisionResult) {
		nlstate->numOuterPages = (int) divisionResult;
	} else {
		nlstate->numOuterPages = (int) divisionResult + 1;
	}
	nlstate->remOuterPages = nlstate->numOuterPages;
	
	nlstate->sqrtOfInnerPages = (int) sqrt(nlstate->innerPageNumber);
	nlstate->xids = palloc(nlstate->sqrtOfInnerPages * sizeof(int));
	nlstate->rewards = palloc(nlstate->sqrtOfInnerPages * sizeof(int));
	nlstate->tidRewards = palloc(nlstate->sqrtOfInnerPages * sizeof(struct tupleRewards));
	nlstate->idxreward = palloc(nlstate->sqrtOfInnerPages * sizeof(struct indexedReward));
	
	int j = 0;
	nlstate->outerpages = palloc((nlstate->numOuterPages) * sizeof(struct outerPgNum));
	for (i = 0; i < (nlstate->numOuterPages); i++) {
		nlstate->outerpages[i].outertupnum = palloc(PAGE_SIZE * sizeof(struct outerTupleNum));
    
		for (j = 0; j < PAGE_SIZE; j++) {
			nlstate->outerpages[i].outertupnum[j].outertupleinfo = palloc(sizeof(struct tupleInfo));
		}
	}

	nlstate->pageIndex = -1;
	nlstate->lastPageIndex = 0;
	nlstate->xidScanKey = (ScanKey) palloc(sizeof(ScanKeyData));
	for (i = 0; i < nlstate->sqrtOfInnerPages; i++) {
		nlstate->tidRewards[i].reward = 0;
		nlstate->tidRewards[i].outpgnum = 0;
		nlstate->tidRewards[i].rewardRatio = 0.0;
	}
	
	for (i = 0; i < (nlstate->numOuterPages); i++) {
		nlstate->outerpages[i].explore_page_reward_ratio = 0.0;
		nlstate->outerpages[i].explore_p_r = 0.0;
		for (j = 0; j < PAGE_SIZE; j++) {
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->explore_num_trails = 0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->explore_success_count = 0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->explore_reward_ratio = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->h_explore = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->p_r = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->exploit_num_trails = 0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->exploit_success_count = 0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->exploit_reward_ratio = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->h_exploit = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->tuple_mean = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->tuple_mean_num = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->tuple_mean_den = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->mean_explore = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->mean_exploit = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->tuple_variance = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->tuple_var_num = 0.0;
			nlstate->outerpages[i].outertupnum[j].outertupleinfo->tuple_var_den = 0.0;
		}
	}

	for (i = 0; i < nlstate->sqrtOfInnerPages; i++) {
		nlstate->idxreward[i].rwrd = 0.0;
		nlstate->idxreward[i].orig_idx = 0;
		nlstate->idxreward[i].rwrdratio = 0.0;
	}
	
	i = 0;
	nlstate->outerPage = CreateRelationPage();
	nlstate->innerPage = CreateRelationPage();

	NL1_printf("ExecInitNestLoop: %s\n",
			"node initialized");
	fastjoin = GetConfigOption("enable_fastjoin", false, false);
	blocknestloop = GetConfigOption("enable_block", false, false);
	if (strcmp(fastjoin, "on") == 0) {
		elog(INFO, "Running bandit join with TID scan..");
	} else {
		if (strcmp(blocknestloop, "on") == 0) {
			elog(INFO, "Running block nested loop..");
		} else {
			elog(INFO, "Running nested loop..");
		}
	}
	if (strcmp(fliporder, "on") == 0) {
		elog(INFO, "flipping inner and outer relations");
	}
	return nlstate;
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void ExecEndNestLoop(NestLoopState *node) {
	int i;
	NL1_printf("ExecEndNestLoop: %s\n",
			"ending node processing");

	PrintNodeCounters(node);
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));

	NL1_printf("ExecEndNestLoop: %s\n",
			"node processing ended");

	i = 0;
	RemoveRelationPage(&(node->outerPage));
	RemoveRelationPage(&(node->innerPage));
	pfree(node->xids);
	pfree(node->rewards);
	pfree(node->xidScanKey);
	pfree(node->tidRewards);
	pfree(node->idxreward);
	
	int j = 0;
	for (i = 0; i < (node->numOuterPages); i++) {
		for (j = 0; j < PAGE_SIZE; j++) {
			pfree(node->outerpages[i].outertupnum[j].outertupleinfo);
		}
		pfree(node->outerpages[i].outertupnum);
	}
	pfree(node->outerpages);
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void ExecReScanNestLoop(NestLoopState *node) {
	PlanState *outerPlan = outerPlanState(node);
	PlanState *innerPlan = innerPlanState(node);
	const char *fliporder;
	fliporder = GetConfigOption("enable_fliporder", false, false);

	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);

	if (strcmp(fliporder, "on") == 0) {
		RemoveRelationPage(&(node->outerPage));
		RemoveRelationPage(&(node->innerPage));
		node->outerPage = CreateRelationPage();
		node->innerPage = CreateRelationPage();
		ExecReScan(innerPlan);
		node->innerTupleCounter = 0;
	}
}

