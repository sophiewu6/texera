import { WorkflowLogicalPlan } from '../../model/workflow-logical-plan';

export const MOCK_WORKFLOW_PLAN: WorkflowLogicalPlan = new WorkflowLogicalPlan(
    [
        {
            'operatorID': 'operator-1',
            'operatorType': 'ScanSource',
            'operatorProperties': { 'tableName': 'twitter_sample' }
        },
        {
            'operatorID': 'operator-2',
            'operatorType': 'ViewResults',
            'operatorProperties': { 'limit': 10, 'offset': 0 }
        }
    ],
    [
        {
            'origin': 'operator-1',
            'destination': 'operator-2'
        }
    ]
);
