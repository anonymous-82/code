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

static void LoadPageWithTIDs(PlanState* outerPlan, struct tupleRewards* tids, RelationPage* relationPage, int index, Relation heapRelation, TupleTableSlot* tup) {
    Buffer tempBuf = InvalidBuffer;
    int i;
    int size;
    TupleTableSlot* tts = tup;
    if(relationPage == NULL) {
        elog(ERROR, "LoadNextOuterPage: null page");
    }
    relationPage->index = 0;

    relationPage->tupleCount = 0;
    size = tids[index].size;
    for (i = 0; i < size; i++) {
        if (!TupIsNull(relationPage->tuples[i])) {
                ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
                relationPage->tuples[i] = NULL;
        }
    }

    for ( i = 0; i < size; i++) {
        while(true) {
            if(heap_fetch(heapRelation, outerPlan->state->es_snapshot, &tids[index].tuples[i], &tempBuf, false, NULL)) {
                ExecStoreTuple(&tids[index].tuples[i],
                        tts,
                        tempBuf,
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

static int popBestTidPageIndex(NestLoopState *node) {
	int i;
	int bestOuterPageIdx = 0;
	int bestInnerPageIdx = 0;
	int bestPageIndex = 0;

	for (i = 0; i < node->activeOuterRelnPages; i++) {
		if (node->outerRewardTuples[i].reward > node->outerRewardTuples[bestOuterPageIdx].reward) {
			bestOuterPageIdx = i;
		}
	}

	for (i = 0; i < node->activeInnerRelnPages; i++) {
		if (node->innerRewardTuples[i].reward > node->innerRewardTuples[bestInnerPageIdx].reward) {
			bestInnerPageIdx = i;
		}
	}

	if (node->innerRewardTuples[bestInnerPageIdx].reward == 0 && node->outerRewardTuples[bestOuterPageIdx].reward == 0){
		if (rand() % 2 == 0){
			bestPageIndex = bestInnerPageIdx;
			bestPageIndex = rand() % node->activeInnerRelnPages;
			node->isBestPageOuter = false;
		}else{
			bestPageIndex = bestOuterPageIdx;
			bestPageIndex = rand() % node->activeOuterRelnPages;
			node->isBestPageOuter = true;
		}
		return bestPageIndex;
	}


	if (node->activeInnerRelnPages != 0 && node->activeOuterRelnPages != 0
			&& node->innerRewardTuples[bestInnerPageIdx].reward > node->outerRewardTuples[bestOuterPageIdx].reward) {
		bestPageIndex = bestInnerPageIdx;
		node->isBestPageOuter = false;
	} else {
		bestPageIndex = bestOuterPageIdx;
		node->isBestPageOuter = true;
	}

	return bestPageIndex;
}

static void storeTIDs(RelationPage* relationPage, struct tupleRewards* tids, int index, int reward) {
    int i = 0;
    tids[index].reward = reward;
    tids[index].size = relationPage->tupleCount;
    for(i = 0; i < relationPage->tupleCount; i++) {
        tids[index].tuples[i] = *relationPage->tuples[i]->tts_tuple;
    }
}

static void PrintNodeCounters(NestLoopState *node) {
	elog(INFO, "Read outer pages: %d", node->outerPageCounter);
	elog(INFO, "Read inner pages: %d", node->innerPageCounter);
	elog(INFO, "Read outer tuples: %ld", node->outerTupleCounter);
	elog(INFO, "Read inner tuples: %ld", node->innerTupleCounter);
	elog(INFO, "Generated joins: %d", node->generatedJoins);
	elog(INFO, "Rescan Count: %d", node->rescanCount);
	elog(INFO, "Current XidPage: %d", node->pageIndex);
	elog(INFO, "Active Outer Relations: %d", node->activeOuterRelnPages);
	elog(INFO, "Active Inner Relations: %d", node->activeInnerRelnPages);
	elog(INFO, "Total page reads: %d", (node->outerPageCounter + node->innerPageCounterTotal));
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
	node->outerss = (ScanState*)outerPlan;
	node->innerss = (ScanState*)innerPlan;

	ResetExprContext(econtext);

	ENL1_printf("entering main loop");

	for (;;) {
		if (node->needOuterPage) {
			if (node->outerTurn) {
				if (!node->greedyExploit && !node->reachedEndOfOuter && node->activeOuterRelnPages < node->sqrtOfInnerPages) {
					node->isExploringOuter = true;
					node->outerPageCounter++;
					if(node->outerPage->tupleCount == 0)
						LoadNextPage(outerPlan, node->outerPage);
					else node->outerPage->index = 0;
					if (node->outerPageCounter >= node->outerPageNumber) {
						elog(INFO, "Reached end of outer");
						node->reachedEndOfOuter = true;
						if (node->outerPage->tupleCount == 0)
							continue;
					}
					node->outerTupleCounter += node->outerPage->tupleCount;
					node->lastReward = 0;
					node->exploreOuterStepCounter = 1;
					node->needOuterPage = false;
					node->needInnerPage = true;
					continue;
				} else if (node->greedyExploit || (!node->reachedEndOfOuter && node->activeOuterRelnPages == node->sqrtOfInnerPages)
						|| (node->reachedEndOfOuter && node->activeOuterRelnPages > 0)) {
					node->greedyExploit = false;
					node->outerPage->index = 0;
					node->isExploringOuter = false;
					node->exploitOuterStepCounter = 0;
					node->pageIndex = popBestTidPageIndex(node);
					if (node->isBestPageOuter) {
						LoadPageWithTIDs(outerPlan, node->outerRewardTuples ,node->outerPage, node->pageIndex, node->outerss->ss_currentRelation, node->outerss->ss_ScanTupleSlot);

						node->outerRewardTuples[node->pageIndex] = node->outerRewardTuples[node->activeOuterRelnPages - 1];
						node->activeOuterRelnPages--;

						node->needOuterPage = false;
						node->needInnerPage = true;
						continue;
					} else {
						LoadPageWithTIDs(innerPlan, node->innerRewardTuples ,node->innerPage, node->pageIndex, node->innerss->ss_currentRelation, node->innerss->ss_ScanTupleSlot);

						node->innerRewardTuples[node->pageIndex] = node->innerRewardTuples[node->activeInnerRelnPages - 1];
						node->activeInnerRelnPages--;

						node->outerTurn = false;
						node->needOuterPage = true;
						node->needInnerPage = false;
						continue;
					}
				} else {
					elog(INFO, "Join finished normally");
					return NULL;
				}
			} else {
				if (node->reachedEndOfOuter) {
					foreach(lc, nl->nestParams)
					{
						NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
						int paramno = nlp->paramno;
						ParamExecData *prm;

						prm = &(econtext->ecxt_param_exec_vals[paramno]);
						Assert(IsA(nlp->paramval, Var));
						Assert(nlp->paramval->varno == INNER_VAR);
						Assert(nlp->paramval->varattno > 0);
						prm->value = slot_getattr(node->innerPage->tuples[0], nlp->paramval->varattno, &(prm->isnull));
						innerPlan->chgParam = bms_add_member(outerPlan->chgParam, paramno);
					}
					ExecReScan(outerPlan);
					node->rescanCount++;
					node->reachedEndOfOuter = false;
				}
				LoadNextPage(outerPlan, node->outerPage);
				if (node->outerPage->tupleCount < PAGE_SIZE) {
					node->reachedEndOfOuter = true;
					if (node->outerPage->tupleCount == 0)
						continue;
				}
				node->needOuterPage = false;
			}
		}
		if (node->needInnerPage) {
			if (node->outerTurn) {
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
				node->needInnerPage = false;
			} else {
				if (!node->greedyExploit && !node->reachedEndOfInner && node->activeInnerRelnPages < node->sqrtOfOuterPages) {
					node->isExploringInner = true;
					node->innerPageCounter++;
					if(node->innerPage->tupleCount == 0)
						LoadNextPage(innerPlan, node->innerPage);
					else node->innerPage->index = 0;
					if (node->innerPageCounter >= node->innerPageNumber) {
						elog(INFO, "Reached end of outer");
						node->reachedEndOfInner = true;
						if (node->innerPage->tupleCount == 0)
							continue;
					}
					node->innerTupleCounter += node->innerPage->tupleCount;
					node->lastReward = 0;
					node->exploreInnerStepCounter = 1;
					node->needInnerPage = false;
					node->needOuterPage = true;
					continue;
				} else if (node->greedyExploit || (!node->reachedEndOfInner && node->activeInnerRelnPages == node->sqrtOfOuterPages)
						|| (node->reachedEndOfInner && node->activeInnerRelnPages > 0)) {
					node->greedyExploit = false;
					node->innerPage->index = 0;
					node->isExploringInner = false;
					node->exploitInnerStepCounter = 0;
					node->pageIndex = popBestTidPageIndex(node);

					if (node->isBestPageOuter) {
						LoadPageWithTIDs(outerPlan, node->outerRewardTuples ,node->outerPage, node->pageIndex, node->outerss->ss_currentRelation, node->outerss->ss_ScanTupleSlot);

						node->outerRewardTuples[node->pageIndex] = node->outerRewardTuples[node->activeOuterRelnPages - 1];
						node->activeOuterRelnPages--;

						node->needOuterPage = false;
						node->needInnerPage = true;
						node->outerTurn = true;
						continue;
					} else {
						LoadPageWithTIDs(innerPlan, node->innerRewardTuples ,node->innerPage, node->pageIndex, node->innerss->ss_currentRelation, node->innerss->ss_ScanTupleSlot);

						node->innerRewardTuples[node->pageIndex] = node->innerRewardTuples[node->activeInnerRelnPages - 1];
						node->activeInnerRelnPages--;

						node->needOuterPage = true;
						node->needInnerPage = false;
						continue;
					}
				} else {
					elog(INFO, "Join finished normally");
					return NULL;
				}
			}
		}
		if (node->outerTurn && node->innerPage->index == node->innerPage->tupleCount) {
			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else {
				if (node->isExploringOuter
						&& ((node->lastReward > 0 && node->exploreOuterStepCounter < node->innerPageNumber)
								|| (node->lastReward == 0 && (++node->nFailure) < N_FAILURE))) { 
					node->outerPage->index = 0;
					node->reward += node->lastReward;
					node->lastReward = 0;
					node->exploreOuterStepCounter++;
					node->needInnerPage = true;
				} else if (node->isExploringOuter && node->exploreOuterStepCounter == node->innerPageNumber) {
					node->needOuterPage = true;
				} else if (node->isExploringOuter && node->lastReward == 0 && (++node->nFailure) >= N_FAILURE) {
					storeTIDs(node->outerPage, node->outerRewardTuples, node->activeOuterRelnPages, node->reward);
					node->nFailure = 0;
					node->activeOuterRelnPages++;
					if(GREEDY && node->reward > 0){
						node->greedyExploit = true;
						node->needOuterPage = true;
					}else{
						node->outerTurn = false;
						node->outerPage->index = 0;
						node->needInnerPage = true;
					}
					node->reward = 0;
				} else if (!node->isExploringOuter && node->exploitOuterStepCounter < node->innerPageNumber) {
					node->outerPage->index = 0;
					node->needInnerPage = true;
					node->exploitOuterStepCounter++;
				} else if (!node->isExploringOuter && node->exploitOuterStepCounter == node->innerPageNumber) {

					node->needOuterPage = true;
				} else {
					elog(ERROR, "Error");
				}
				continue;
			}
		}

		if (!node->outerTurn && node->outerPage->index == node->outerPage->tupleCount) {
			if (node->innerPage->index < node->innerPage->tupleCount - 1) {
				node->innerPage->index++;
				node->outerPage->index = 0;
			} else {
				if (node->isExploringInner &&
						((node->lastReward > 0 && node->exploreInnerStepCounter < node->outerPageNumber)
								|| (node->lastReward == 0 && (++node->nFailure) < N_FAILURE))) { 
					node->innerPage->index = 0;
					node->reward += node->lastReward;
					node->lastReward = 0;
					node->exploreInnerStepCounter++;
					node->needOuterPage = true;
				} else if (node->isExploringInner && node->exploreInnerStepCounter == node->outerPageNumber) {
					node->needInnerPage = true;
				} else if (node->isExploringInner && node->lastReward == 0 && (++node->nFailure) >= N_FAILURE) {
					storeTIDs(node->innerPage, node->innerRewardTuples, node->activeInnerRelnPages, node->reward);
					node->nFailure = 0;
					node->activeInnerRelnPages++;
					if(GREEDY && node->reward > 0){
						node->greedyExploit = true;
						node->needInnerPage = true;
					}else{
						node->outerTurn = true;
						node->needOuterPage = true;
						node->innerPage->index = 0;
					}
					node->reward = 0;
				} else if (!node->isExploringInner && node->exploitInnerStepCounter < node->outerPageNumber) {
					node->innerPage->index = 0;
					node->needOuterPage = true;
					node->exploitInnerStepCounter++;
				} else if (!node->isExploringInner && node->exploitInnerStepCounter == node->outerPageNumber) {
					node->needInnerPage = true;
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
		if (node->outerTurn) {
			node->innerPage->index++;
			if (TupIsNull(innerTupleSlot)) {
				elog(WARNING, "inner tuple is null");
				return NULL;
			}
			if (TupIsNull(outerTupleSlot)) {
				if (node->activeOuterRelnPages > 0) { 
					elog(INFO, "Null outer detected");
					node->needOuterPage = true;
					continue;
				}
				return NULL;
			}
		} else {
			node->outerPage->index++;
			if (TupIsNull(outerTupleSlot)) {
				elog(WARNING, "outer tuple is null");
				return NULL;
			}
			if (TupIsNull(innerTupleSlot)) {
				if (node->activeInnerRelnPages > 0) { 
					elog(INFO, "Null inner detected");
					node->needInnerPage = true;
					continue;
				}
				return NULL;
			}
		}

		ENL1_printf("testing qualification");
		if (ExecQual(joinqual, econtext)) {

			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");
				node->lastReward++;
				node->generatedJoins++;
				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);

		ResetExprContext(econtext);ENL1_printf("qualification failed, looping");
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

	CHECK_FOR_INTERRUPTS();ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = innerPlanState(node);
	innerPlan = outerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	ResetExprContext(econtext);ENL1_printf("entering main loop");

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
		ResetExprContext(econtext);ENL1_printf("qualification failed, looping");
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

	CHECK_FOR_INTERRUPTS();ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	ResetExprContext(econtext);ENL1_printf("entering main loop");

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
				}ENL1_printf("rescanning inner plan");
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
		ResetExprContext(econtext);ENL1_printf("qualification failed, looping");
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
				continue;
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
NestLoopState* ExecInitNestLoop(NestLoop *node, EState *estate, int eflags) {
	NestLoopState *nlstate;
	const char *fastjoin;
	const char *blocknestloop;
	const char *fliporder;
	int i;

	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	NL1_printf("ExecInitNestLoop: %s\n",
			"initializing node");

	nlstate = makeNode(NestLoopState);
	nlstate->js.ps.plan = (Plan*) node;
	nlstate->js.ps.state = estate;
	nlstate->js.ps.ExecProcNode = ExecNestLoop;

	ExecAssignExprContext(estate, &nlstate->js.ps);

	outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
	if (node->nestParams == NIL) {
		eflags |= EXEC_FLAG_REWIND;
		elog(INFO, "nestParams NIL");
	} else
		eflags &= ~EXEC_FLAG_REWIND;
	innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);

	ExecInitResultTupleSlotTL(estate, &nlstate->js.ps);
	ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

	if (node->join.plan.qual == NIL) {
		elog(INFO, "join.plan.qual NIL");
	}
	if (node->join.joinqual == NIL) {
		elog(INFO, "join.joinqual NIL");
	}
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
		elog(ERROR, "unrecognized join type: %d", (int ) node->join.jointype);
	}

	nlstate->nl_NeedNewOuter = true;
	nlstate->nl_MatchedOuter = false;

	fliporder = GetConfigOption("enable_fliporder", false, false);
	nlstate->activeOuterRelnPages = 0;
	nlstate->isExploringOuter = true;
	nlstate->lastReward = 0;
	nlstate->needOuterPage = true;
	nlstate->needInnerPage = true;
	nlstate->outerTurn = true;
	nlstate->exploitOuterStepCounter = 0;
	nlstate->innerPageCounter = 0;
	nlstate->innerPageCounterTotal = 0;
	nlstate->outerPageCounter = 0;
	nlstate->reachedEndOfOuter = false;
	nlstate->reachedEndOfInner = false;
	nlstate->innerTupleCounter = 0;
	nlstate->outerTupleCounter = 0;
	nlstate->generatedJoins = 0;
	nlstate->rescanCount = 0;
	nlstate->outerStartKeyValue = 1;
	nlstate->outerEndKeyValue = nlstate->outerStartKeyValue;
	nlstate->greedyExploit = false;
	nlstate->nFailure = 0;

	if (strcmp(fliporder, "on") == 0) {
		nlstate->outerPageNumber = innerPlan(node)->plan_rows / PAGE_SIZE + 1;
		nlstate->innerPageNumber = outerPlan(node)->plan_rows / PAGE_SIZE + 1;
	} else {
		nlstate->outerPageNumber = outerPlan(node)->plan_rows / PAGE_SIZE + 1;
		nlstate->innerPageNumber = innerPlan(node)->plan_rows / PAGE_SIZE + 1;
	}
	elog(INFO, "Outer page number: %ld", nlstate->outerPageNumber);
	elog(INFO, "Inner page number: %ld", nlstate->innerPageNumber);

	nlstate->sqrtOfOuterPages = (int) (sqrt(nlstate->outerPageNumber)/20);
	nlstate->sqrtOfInnerPages = (int) (sqrt(nlstate->innerPageNumber)/20);
	nlstate->outerRewardTuples = palloc((nlstate->sqrtOfInnerPages) * sizeof(struct tupleRewards));
	nlstate->innerRewardTuples = palloc((nlstate->sqrtOfOuterPages) * sizeof(struct tupleRewards));

    for(i = 0; i < nlstate->sqrtOfInnerPages; i++) {
        nlstate->outerRewardTuples[i].reward = 0;
    }
	for(i = 0; i < nlstate->sqrtOfOuterPages; i++) {
		nlstate->innerRewardTuples[i].reward = 0;
	}

	nlstate->pageIndex = -1;
	nlstate->lastPageIndex = 0;
	nlstate->outerPage = CreateRelationPage();
	nlstate->innerPage = CreateRelationPage();

	NL1_printf("ExecInitNestLoop: %s\n",
			"node initialized");
	fastjoin = GetConfigOption("enable_fastjoin", false, false);
	blocknestloop = GetConfigOption("enable_block", false, false);
	if (strcmp(fastjoin, "on") == 0) {
		elog(INFO, "Running bandit join..");
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

	RemoveRelationPage(&(node->outerPage));
	RemoveRelationPage(&(node->innerPage));
	pfree(node->outerRewardTuples);
	pfree(node->innerRewardTuples);
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

