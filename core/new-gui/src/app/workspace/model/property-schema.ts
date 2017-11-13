export class PropertySchema {
    constructor(
        public readonly propertyName: string,
        public readonly propertyType: string,
        public readonly defaultValue?: any,
        public readonly propertyDescription?: string
    ) {
    }
}
