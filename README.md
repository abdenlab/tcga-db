# GDC metadata

## Resources

https://gdc.cancer.gov/about-data/gdc-data-processing/resources-tcga-users
https://gdc.cancer.gov/resources-tcga-users/tcga-code-tables
https://docs.gdc.cancer.gov/Encyclopedia/pages/TCGA_Barcode/#reading-barcodes

https://docs.gdc.cancer.gov/Encyclopedia/
https://docs.gdc.cancer.gov/Encyclopedia/pages/GDC_API/

https://docs.gdc.cancer.gov/API/Users_Guide/Getting_Started/
https://docs.gdc.cancer.gov/API/Users_Guide/Search_and_Retrieval
https://docs.gdc.cancer.gov/API/Users_Guide/Python_Examples/
https://docs.gdc.cancer.gov/API/Users_Guide/GraphQL_Examples/
https://docs.gdc.cancer.gov/API/Users_Guide/Additional_Examples/
https://docs.gdc.cancer.gov/API/Users_Guide/Appendix_A_Available_Fields/
https://docs.gdc.cancer.gov/API/Users_Guide/Appendix_B_Key_Terms/
https://docs.gdc.cancer.gov/API/Users_Guide/Appendix_C_Format_of_Submission_Requests_and_Responses/

https://docs.gdc.cancer.gov/Data/Data_Model/GDC_Data_Model/
https://docs.gdc.cancer.gov/Data_Dictionary/
https://docs.gdc.cancer.gov/Data_Dictionary/viewer/
https://github.com/NCI-GDC/gdcdictionary/tree/develop/src/gdcdictionary/schemas

https://gdc.cancer.gov/developers/gdc-application-programming-interface-api
https://github.com/NCI-GDC/gdc-frontend-framework/blob/3aeb60a6/packages/core/src/features/gdcapi/gdcapi.ts#L96

## Endpoints

* `projects`: Information about projects
* `cases`: Information related to cases, or sample donors
* `files`: Information about files stored in the GDC
* `history`: Information related to file version history
* `annotations`: Information about annotations to GDC data

"Search box" endpoint (via `query` parameter): 

```
https://api.gdc.cancer.gov/v0/all?query=TCGA&size=5
```

## Information about available fields

```
https://api.gdc.cancer.gov/{endpoint}/_mapping
```

BCR = "Biospecimen Core Resource"
