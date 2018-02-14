export class WorkflowLogicalPlan {

    operatorIDMap = new Map<string, OperatorPredicate>();

    constructor(
        public operatorPredicates: OperatorPredicate[],
        public operatorLinks: OperatorLink[]
    ) {
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

    hasLink(operatorLink: OperatorLink): boolean {
        return this.operatorLinks.filter(link => link.origin === operatorLink.origin 
            && link.destination === operatorLink.destination).length !== 0;
    }

    deleteOperator(operatorID: string) {
        // find all links related to this operatorID
        const relatedLinks = this.operatorLinks.filter(
            link => link.origin === operatorID
                || link.destination === operatorID
        );

        // delete operator links from this.operatorLinks
        relatedLinks.forEach(this.deleteLink);

        // delete operator from ID map and predicate list
        this.operatorPredicates = this.operatorPredicates.filter(
            predicate => predicate.operatorID !== operatorID)
        ;
        this.operatorIDMap.delete(operatorID);
    }

    deleteLink(linkToDelete: OperatorLink) {
        this.operatorLinks = this.operatorLinks.filter(
            link => link.origin === linkToDelete.origin
                && link.destination === linkToDelete.destination
        );
    }
}
