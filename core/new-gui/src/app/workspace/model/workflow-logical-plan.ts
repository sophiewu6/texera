import OperatorPredicate from './operator-predicate';
import OperatorLink from './operator-link';

export default class WorkflowLogicalPlan {

    operatorIDMap = new Map<string, OperatorPredicate>();

    constructor(
        public operatorPredicates?: OperatorPredicate[],
        public operatorLinks?: OperatorLink[]
    ) {
        if (operatorPredicates) {
            operatorPredicates.forEach(op => this.operatorIDMap[op.operatorID] = op);
        }
     }

     addOperator(operatorID: string, operatorType: string, operatorPredicate?: OperatorPredicate) {
         if (! operatorPredicate) {
             operatorPredicate = new OperatorPredicate(operatorID, operatorType, this.generateOperatorProperty(operatorType));
         }
         this.operatorIDMap[operatorID] = operatorPredicate;
     }

     private generateOperatorProperty(operatorType: string) {
         return {'testProperty': 'testValue'};
     }
}
