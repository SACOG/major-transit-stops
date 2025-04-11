# Major Transit Stops for AB2097 parking requirement exemptions (Enacted 2022)


## Definition of a "Major" Transit Stop
Per [Section 21064.3 of the California Public Resources Code (PRC)](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=PRC&sectionNum=21064.3.), a "major" transit stop is defined as:
* An existing rail or bus rapid transit station.
* A ferry terminal served by either a bus or rail transit service.
* The intersection of two or more major bus routes with a frequency of service interval of 15 minutes or less during the morning and afternoon peak commute periods.

## Key Laws Affected by Major Transit Stops
* [AB2097](https://leginfo.legislature.ca.gov/faces/billTextClient.xhtml?bill_id=202120220AB2097), passed in November 2022, which states that public agencies "...shall not impose or enforce any minimum automobile parking requirement on a residential, commercial, or other development project if the project is located within one-half mile of public transit." With "public transit" defined as a major transit stop. Importantly, [PRC 21155](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=PRC&sectionNum=21155.), for purposes of designating a given stop as a major stop, uses existing transit service *plus* service assumed in "the applicable regional transportation plan". For example, if an intersection currently does not have transit service, but per a regional transportation plan will have bus rapid transit (BRT), then the area within a half-mile of that intersection would be subject to AB2097's removal of parking restrictions.


## Identifying Which SACSIM Transit Stops are Major Transit Stops
[major_stop_identifier.py](https://github.com/SACOG/major-transit-stops/blob/main/AB2097/major_stop_identifier.py) takes in a SACSIM transit line file (.LIN or .TXT) and flags which stop nodes qualify as "major" stops per the PRC definition described [above](#Definition).

**IMPORTANTLY**, this script should only be used for assessing *future-year* major stops. If you want to know which existing transit stops qualify as major stops, you should use a different process that parses General Transit Feed Specification (GTFS) data, such as the work done by the [Cal-ITP team](https://github.com/cal-itp/data-analyses/tree/main/high_quality_transit_areas).
