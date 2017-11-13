import { PropertySchema } from './property-schema';

export class OperatorSchema {
    constructor(
        public readonly operatorType: string,
        public readonly userFriendlyName: string,
        public readonly properties: PropertySchema[],
        public readonly operatorDescription?: string,
        public readonly requiredProperties?: string[],
        public readonly advancedOptions?: string[]
    ) {
    }

}
