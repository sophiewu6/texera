import { JSONSchema4 } from 'json-schema';

/**
 * This file contains multiple type declarations related to operator schema.
 * These type declarations should be the same with the backend API.
 *
 * This file include a sample mock data:
 *   workspace/service/operator-metadata/mock-operator-metadata.data.ts
 *
 */

// Property description is a map from attribute to its description string
export interface OperatorPropertyDescription extends Readonly <{
  [attribute: string]: string;
}> {}

export interface OperatorAdditionalMetadata extends Readonly<{
  userFriendlyName: string;
  numInputPorts: number;
  numOutputPorts: number;
  operatorGroupName: string;
  advancedOptions?: ReadonlyArray<string>;
  operatorDescription?: string;
  property_description: OperatorPropertyDescription;
}> { }

export interface OperatorSchema extends Readonly<{
  operatorType: string;
  jsonSchema: Readonly<JSONSchema4>;
  additionalMetadata: OperatorAdditionalMetadata;
}> { }

export interface GroupInfo extends Readonly<{
  groupName: string;
  groupOrder: number;
}> { }

export interface OperatorMetadata extends Readonly<{
  operators: ReadonlyArray<OperatorSchema>;
  groups: ReadonlyArray<GroupInfo>;
}> { }


