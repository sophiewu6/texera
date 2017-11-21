import { PropertySchema } from './property-schema';

export class OperatorSchema {
    constructor(
        public readonly operatorType: string,
        public readonly userFriendlyName: string,
        public readonly properties: PropertySchema[],
        public readonly numInputPorts: number,
        public readonly numOutputPorts: number,
        public readonly operatorDescription?: string,
        public readonly requiredProperties?: string[],
        public readonly advancedOptions?: string[]
    ) {
    }

    generateSchemaObject(): Object {
        const schemaProperties = {};
        this.properties.forEach(x => schemaProperties[x.propertyName] = {'type': x.propertyType});
        return {
            'type': 'object',
            'properties': schemaProperties
        };
    }

}
