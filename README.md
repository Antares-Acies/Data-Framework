# Data Framework

## Overview
This framework is designed to process and transform data through a series of structured steps. It is divided into three main Steps: Source Filter, Source Validation, and Transformation. The final output is the transformed data, which is saved in the database.

### Transformation and Validation Logic
- **Transformation Logic**: The code applies specific transformation rules to the data, as defined in the `transformation_mapping` sheet. These rules modify the data to meet the desired format and structure.
- **Validation Logic**: The code validates the data against criteria specified in the `source_data_validation` sheet. This step ensures that the data is accurate and free from errors before further processing.

## Introduction of the Sheets
### Role and Responsibilities
- **global_parameter Sheet**: Contains global parameters used to filter the extracted data.
- **source_data_validation Sheet**: Lists the validation rules that the data must pass to be considered error-free.
- **transformation_mapping Sheet**: Defines the transformation rules to be applied to the data.
- **Additional Sheets**: 
  - **rule_group_definition**: Defines groups of rules for data transformation.
  - **conditional_rule_definition**: Specifies conditional rules for data transformation.
  - **master_mapping_rule_definition**: Contains master mapping rules for data transformation.
  - **master_repository**: Stores master data used in the `master_mapping_rule_definition`.

### How These Sheets Work
- **global_parameter Sheet**: Filters the raw data based on predefined global parameters.
- **source_data_validation Sheet**: Ensures data integrity by applying validation rules.
- **transformation_mapping Sheet**: Guides the transformation process by specifying how data should be modified.
- **Additional Sheets**: Support the transformation process with extra data and rules.

## Data Framework
### Context and Management
The framework is divided into three main parts:

1. **Source Filter**:
   - Extracts data from two types of sources: databases and SFTP server.
   - Filters the data using parameters from the `global_parameter` sheet.
   - Saves the filtered data in Parquet format.

2. **Source Validation**:
   - Reads the filtered data saved in Parquet format.
   - Applies validation rules from the `source_data_validation` sheet.
   - Saves the validated, error-free data in Parquet format.

3. **Transformation**:
   - Reads the error-free data.
   - Applies transformation rules from the `transformation_mapping` sheet, along with additional rules from `rule_group_definition`, `conditional_rule_definition`, `master_mapping_rule_definition`, and `master_repository`.
   - Saves the transformed data in the database.

## Conclusion
This framework ensures a systematic approach to data processing, from extraction and filtering to validation and transformation, resulting in a meaningful, transformed data ready for use.