package edu.uci.ics.texera.dataflow.plangen.schemaPropagation;

import edu.uci.ics.texera.dataflow.common.PredicateBase;
import org.jetbrains.annotations.NotNull;

public class OperatorValidationResult {

    private PredicateBase operatorPredicate;
    private String errorMessage;

    public static OperatorValidationResult fromOperator(@NotNull PredicateBase operatorPredicate) {
        return new OperatorValidationResult(operatorPredicate, null);
    }

    public static OperatorValidationResult fromError(@NotNull String errorMessage) {
        return new OperatorValidationResult(null, errorMessage);

    }

    private OperatorValidationResult(PredicateBase operatorPredicate, String errorMessage) {
        this.operatorPredicate = operatorPredicate;
        this.errorMessage = errorMessage;
    }

    public PredicateBase getOperatorPredicate() {
        return operatorPredicate;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

}
