import { OperatorPredicate } from './operator-predicate';
import { OperatorLink } from './operator-link';

export class WorkflowLogicalPlan {

    operatorIDMap = new Map<string, OperatorPredicate>();

    constructor(
        public operatorPredicates?: OperatorPredicate[],
        public operatorLinks?: OperatorLink[]
    ) {
        if (operatorPredicates) {
            operatorPredicates.forEach(op => this.operatorIDMap[op.operatorID] = op);
        }
     }

     addOperator(operatorID: string, operatorType: string, operatorPredicate: OperatorPredicate) {
         this.operatorIDMap[operatorID] = operatorPredicate;
     }
}
