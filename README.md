# AWSCostOptimization
Documents and Scripts to reduce AWS Costs


This is a project to use Pandas to do AWS Cost Analysis and uses the approach in the PDF document in this repository.

The script collects data from Cost Explorer API and creates some report sheets for excel file and then downloads the CUR data from the AWS account to show monthly Data Transfer reports, and Per resource cost in the account.

For a Multi-account setup it has a report which groups the data per account number and aggregates them per service and per resource.


Command example:-

#### Example command to processe 2 months of data

python CostOptimization.py --months 2
