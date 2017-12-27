import { WorkflowLogicalPlan } from '../../model/workflow-logical-plan';
import { OperatorPredicate } from '../../model/operator-predicate';
import { OperatorLink } from '../../model/operator-link';

export const MOCK_WORKFLOW_PLAN: WorkflowLogicalPlan = new WorkflowLogicalPlan(
    [
        new OperatorPredicate('operator-1', 'ScanSource', { 'tableName': 'twitter_sample' }),
        new OperatorPredicate('operator-2', 'ViewResults', { 'limit': 10, 'offset': 0 })
    ],
    [
        new OperatorLink('operator-1', 'operator-2')
    ]
);
