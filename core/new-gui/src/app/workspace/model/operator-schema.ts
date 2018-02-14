interface OperatorAdditionalMetadata {
    userFriendlyName: string;
    numInputPorts: number;
    numOutputPorts: number;
    advancedOptions?: string[];
    operatorGroupName?: string;
    operatorDescription?: string;
}

interface OperatorSchema {
    operatorType: string;
    jsonSchema: Object;
    additionalMetadata: OperatorAdditionalMetadata;
}
