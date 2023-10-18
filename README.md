# Sample Workflow for DSWx-SAR

This provides end-to-end workflow for [DSWx-SAR](https://github.com/opera-adt/DSWX-SAR/tree/main) written by Jungkyo Jung and OPERA ADT. The purpose is for validation and inspection.

Mostly experimental.

# Installation and Use

1. Clone repository
2. Install `dswx_val` environment via `mamba env update -f environment.yml`
3. Install the jupyter kernel via `python -m ipykernel install --user --name dswx_val`

Launch via `jupyter-lab`. Basically takes a single Sentinel-1 SLC ID and obtains the water products for the associated input granule. Here is a diagram for accounting:

$$
1 \textrm{ Sentinel-1 SLC ID} \longrightarrow N \textrm{ OPERA RTC Burst Products} \longrightarrow M \textrm{ MGRS Tiled DSWx Products}
$$
where $N\geq24$ (at least 8 bursts per swath) and $M \leq 9$ (don't know exact size comparison between MGRS tile and S1 acq; this is a much larger upper bound assuming 9 overlapping tiles if acquisition occurs in center of MGRS tile and has some overlap across all 8 neighbors; likely 3-6).

May be updated to take in specific burst RTC products and remove dependence on access to PST Venue ([reference](https://github.com/OPERA-Cal-Val/DSWx-Requirement-Verification#setup-for-validation-table-generation)). This requires a `.env` file with JPL credentials.


# Notes

I am localizing the data and hoping that based on the general comments of the RunConfig templates that I am getting it right. I am currently computing HAND on the fly, though I could be doing it slightly differently than expected as it appears there is an open PR to download hand from ASF.
