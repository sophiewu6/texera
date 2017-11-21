import { OperatorPredicate } from './operator-predicate';
import { OperatorLink } from './operator-link';

export class WorkflowLogicalPlan {

    operatorIDMap = new Map<string, OperatorPredicate>();

    constructor(
        public operatorPredicates?: OperatorPredicate[],
        public operatorLinks?: OperatorLink[]
    ) {
        if (!operatorPredicates) {
            this.operatorPredicates = [];
        }
        if (!operatorLinks) {
            this.operatorLinks = [];
        }
        this.operatorPredicates.forEach(op => this.operatorIDMap[op.operatorID] = op);
    }

    getOperator(operatorID: string): OperatorPredicate {
        return this.operatorIDMap.get(operatorID);
    }

    addOperator(operatorID: string, operatorType: string, operatorPredicate: OperatorPredicate): void {
        this.operatorIDMap.set(operatorID, operatorPredicate);
        this.operatorPredicates.push(operatorPredicate);
    }

    hasOperator(operatorID: string): boolean {
        return this.operatorIDMap.has(operatorID);
    }

    addLink(operatorLink: OperatorLink): void {
        this.operatorLinks.push(operatorLink);
    }

    deleteOperator(operatorID: string) {
        // TODO
    }
}
