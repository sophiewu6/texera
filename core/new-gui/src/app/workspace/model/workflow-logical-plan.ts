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

    addOperator(operator: OperatorPredicate): void {
        this.operatorIDMap.set(operator.operatorID, operator);
        this.operatorPredicates.push(operator);
    }

    hasOperator(operatorID: string): boolean {
        return this.operatorIDMap.has(operatorID);
    }

    addLink(operatorLink: OperatorLink): void {
        this.operatorLinks.push(operatorLink);
    }

    deleteOperator(operatorID: string) {

        // get predicate index and delete it from the list
        const predicateIndex = this.getOperatorPredicateIndex(operatorID);
        this.operatorPredicates.splice(predicateIndex, 1);

        // delete operator links from this.operatorLinks

        this.deleteLink(operatorID);

        // delete operator from ID map
        this.operatorIDMap.delete(operatorID);
    }

    deleteLink(operatorID: string) {

        if (!this.hasOperator(operatorID)) {
            return;
        }

        const deleteIndexList = [];
        // find all the links related to the deleted operator
        for (let i = 0; i < this.operatorLinks.length; ++i) {
            const currentLink = this.operatorLinks[i];
            if (currentLink.origin === operatorID || currentLink.destination === operatorID) {
                deleteIndexList.push(i);
            }
        }
        // delete from the end so index will not mess up when deleting
        for (let i = deleteIndexList.length - 1; i >= 0; --i) {
            this.operatorLinks.splice(deleteIndexList[i], 1);
        }
    }

    getOperatorPredicateIndex(operatorID: string) {
        let index = 0;
        // find the index of the deleted operator in the predicate list
        if (this.hasOperator(operatorID)) {
            for (let i = 0; i < this.operatorPredicates.length; ++i) {
                if (this.operatorPredicates[i].operatorID === operatorID) {
                    index = i;
                    break;
                }
            }
        }
        return index;
    }
}
