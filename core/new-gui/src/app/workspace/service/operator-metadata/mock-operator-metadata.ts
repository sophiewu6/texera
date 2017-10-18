import PropertySchema from '../../model/property-schema';
import OperatorSchema from '../../model/operator-schema';

export const OPERATOR_METADATA: OperatorSchema[] = [
    new OperatorSchema(
        'ScanSource',
        'Source: Scan',
        [ new PropertySchema('tableName', 'string') ],
        'Read records from a table one by one'
    ),
    new OperatorSchema(
        'ViewResults',
        'View Results',
        [
            new PropertySchema('limit', 'integer', 10),
            new PropertySchema('offset', 'integer', 0)
        ],
        'View the results of a workflow'
    )
];
