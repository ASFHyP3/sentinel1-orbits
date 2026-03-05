# Sentinel-1 Orbits

Applications supporting the [Sentinel-1 Precise Orbit Determination (POD) products](https://documentation.dataspace.copernicus.eu/Data/SentinelMissions/Sentinel1.html#sentinel-1-precise-orbit-determination-pod-products) dataset in the Registry of Open Data on AWS.

## Deployments

| Environment | URL |
|-------------|-----|
| prod | https://s1-orbits.asf.alaska.edu |
| test | https://s1-orbits-test.asf.alaska.edu |

## Usage

There is a Python package available at [sentinel1-orbits-py](https://github.com/ASFHyP3/sentinel1-orbits-py), or the API can be queried directly to get an orbit file for a specific scene:

```
https://s1-orbits.asf.alaska.edu/scene/{granule_id}
```

### Example

Getting the orbit file for `S1B_IW_GRDH_1SDV_20211013T183321_20211013T183346_029121_037995_5B38`:

```bash
curl -L https://s1-orbits.asf.alaska.edu/scene/S1B_IW_GRDH_1SDV_20211013T183321_20211013T183346_029121_037995_5B38
```
