# SF1 TPC-DS

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.tpcds`.

Workflow source revision `203cab081197d8a74ab170bac04f6a183144d826`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, duckdb 1.4.4, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

The suite generates 24 synthetic SF1 tables with the signed TPC-DS extension and loads Parquet into format v2 tables. Each timing cell contains median warm wall seconds, CPU-seconds and sampled peak MiB after comparison with Spark. Dashes indicate unavailable measurements.

**Verdicts.** 99 unverified.

| Query | Sail verdict | Comet verdict | Spark wall/CPU/MiB | Spark+Comet wall/CPU/MiB | Sail wall/CPU/MiB |
| --- | --- | --- | --- | --- | --- |
| 1 | unverified | match | 1.153/3.340/4,762.6 | 1.104/3.435/1,336.7 | —/—/— |
| 2 | unverified | match | 1.198/3.305/4,371.7 | 1.135/3.495/1,601.1 | —/—/— |
| 3 | unverified | match | 0.559/1.560/4,380.6 | 0.408/1.245/1,654.0 | —/—/— |
| 4 | unverified | match | 5.017/14.795/4,479.3 | 3.321/10.470/2,832.7 | —/—/— |
| 5 | unverified | match | 1.462/3.905/4,575.2 | 1.100/3.085/3,244.0 | —/—/— |
| 6 | unverified | match | 1.192/3.460/4,582.3 | 0.715/1.945/3,366.2 | —/—/— |
| 7 | unverified | match | 1.066/3.150/4,643.5 | 0.724/2.095/3,663.0 | —/—/— |
| 8 | unverified | match | 0.656/1.615/4,650.6 | 0.616/1.660/3,642.6 | —/—/— |
| 9 | unverified | match | 1.866/4.335/4,674.4 | 1.436/3.945/3,714.5 | —/—/— |
| 10 | unverified | match | 1.353/3.360/4,882.4 | 0.824/2.240/3,879.5 | —/—/— |
| 11 | unverified | match | 2.245/6.770/5,849.6 | 1.554/4.730/3,942.0 | —/—/— |
| 12 | unverified | match | 0.335/0.745/5,851.3 | 0.248/0.515/3,942.1 | —/—/— |
| 13 | unverified | match | 0.859/2.175/5,844.3 | 0.497/1.200/3,946.4 | —/—/— |
| 14 | unverified | match | 8.953/23.145/5,645.3 | 5.939/13.095/5,828.5 | —/—/— |
| 15 | unverified | match | 0.459/1.130/4,944.7 | 0.266/0.535/5,883.4 | —/—/— |
| 16 | unverified | match | 0.939/2.495/4,941.6 | 0.624/1.485/5,874.5 | —/—/— |
| 17 | unverified | match | 2.039/6.440/4,945.4 | 1.019/2.720/5,923.1 | —/—/— |
| 18 | unverified | match | 1.256/3.435/4,950.2 | 0.716/1.675/5,946.1 | —/—/— |
| 19 | unverified | match | 0.462/1.045/4,960.2 | 0.347/0.720/5,929.1 | —/—/— |
| 20 | unverified | match | 0.299/0.650/4,961.1 | 0.256/0.550/5,891.3 | —/—/— |
| 21 | unverified | match | 0.522/1.265/5,003.3 | 0.410/0.715/6,047.8 | —/—/— |
| 22 | unverified | match | 1.151/2.475/5,009.0 | 0.508/0.960/5,993.7 | —/—/— |
| 23 | unverified | match | 6.237/18.690/6,778.5 | 2.846/6.940/3,864.3 | —/—/— |
| 24 | unverified | match | 1.625/3.490/7,207.0 | 1.384/2.805/4,003.1 | —/—/— |
| 25 | unverified | match | 1.949/5.805/5,567.2 | 1.000/2.880/4,291.0 | —/—/— |
| 26 | unverified | match | 0.641/1.695/5,573.3 | 0.435/0.980/4,359.0 | —/—/— |
| 27 | unverified | match | 0.977/2.715/5,580.2 | 0.599/1.530/4,492.9 | —/—/— |
| 28 | unverified | match | 1.093/3.020/5,588.5 | 0.556/1.345/4,548.2 | —/—/— |
| 29 | unverified | match | 1.808/5.760/5,591.0 | 0.930/2.600/4,524.3 | —/—/— |
| 30 | unverified | match | 0.603/1.495/5,597.3 | 0.430/0.940/4,524.8 | —/—/— |
| 31 | unverified | match | 1.437/3.575/6,030.7 | 0.805/1.750/4,525.4 | —/—/— |
| 32 | unverified | match | 0.570/1.560/4,812.0 | 0.274/0.440/4,525.8 | —/—/— |
| 33 | unverified | match | 0.825/1.785/3,590.3 | 0.658/1.300/4,526.4 | —/—/— |
| 34 | unverified | match | 0.499/1.255/3,590.3 | 0.329/0.765/4,528.9 | —/—/— |
| 35 | unverified | match | 0.984/2.340/3,590.2 | 0.642/1.480/4,535.2 | —/—/— |
| 36 | unverified | match | 0.493/1.185/3,590.8 | 0.311/0.630/4,538.2 | —/—/— |
| 37 | unverified | match | 0.239/0.510/3,598.7 | 0.159/0.275/4,538.3 | —/—/— |
| 38 | unverified | match | 0.932/2.420/3,599.2 | 0.529/0.965/4,539.1 | —/—/— |
| 39 | unverified | match | 1.750/4.670/3,736.6 | 1.170/2.755/4,887.6 | —/—/— |
| 40 | unverified | match | 0.570/1.390/3,880.9 | 0.295/0.630/4,558.0 | —/—/— |
| 41 | unverified | match | 0.190/0.325/3,294.7 | 0.194/0.450/4,553.3 | —/—/— |
| 42 | unverified | match | 0.343/0.920/3,294.9 | 0.231/0.405/4,553.6 | —/—/— |
| 43 | unverified | match | 0.399/1.045/3,468.7 | 0.204/0.395/4,553.7 | —/—/— |
| 44 | unverified | match | 0.310/0.525/3,457.0 | 0.368/0.845/4,554.4 | —/—/— |
| 45 | unverified | match | 0.389/0.760/3,467.2 | 0.281/0.535/4,554.9 | —/—/— |
| 46 | unverified | match | 0.541/1.445/3,753.4 | 0.374/0.830/4,471.8 | —/—/— |
| 47 | unverified | match | 1.160/2.950/3,870.1 | 0.741/1.575/4,475.2 | —/—/— |
| 48 | unverified | match | 0.655/1.865/3,873.5 | 0.420/0.965/4,477.1 | —/—/— |
| 49 | unverified | match | 0.998/2.515/3,443.4 | 0.594/1.230/4,477.8 | —/—/— |
| 50 | unverified | match | 0.654/1.410/3,602.4 | 0.349/0.665/4,495.9 | —/—/— |
| 51 | unverified | match | 1.369/4.295/3,615.3 | 0.616/1.590/4,493.8 | —/—/— |
| 52 | unverified | match | 0.315/0.760/3,615.2 | 0.177/0.280/4,493.8 | —/—/— |
| 53 | unverified | match | 0.407/0.945/3,625.1 | 0.246/0.430/4,494.1 | —/—/— |
| 54 | unverified | match | 1.106/2.965/3,119.3 | 0.624/1.440/4,499.0 | —/—/— |
| 55 | unverified | match | 0.307/0.700/3,326.5 | 0.181/0.295/4,499.2 | —/—/— |
| 56 | unverified | match | 0.870/2.040/3,612.5 | 0.571/0.990/4,499.6 | —/—/— |
| 57 | unverified | match | 0.903/2.215/3,456.5 | 0.542/0.835/4,500.4 | —/—/— |
| 58 | unverified | match | 0.938/2.245/4,219.5 | 0.598/1.055/4,500.8 | —/—/— |
| 59 | unverified | match | 0.587/1.485/4,235.2 | 0.416/0.740/4,500.9 | —/—/— |
| 60 | unverified | match | 0.832/1.945/4,078.7 | 0.557/0.945/4,501.2 | —/—/— |
| 61 | unverified | match | 0.369/0.720/4,078.7 | 0.353/0.630/4,501.4 | —/—/— |
| 62 | unverified | match | 0.387/0.930/4,078.8 | 0.235/0.400/4,438.8 | —/—/— |
| 63 | unverified | match | 0.347/0.765/4,081.8 | 0.258/0.430/4,438.8 | —/—/— |
| 64 | unverified | match | 3.445/9.270/4,660.6 | 2.266/4.610/5,244.2 | —/—/— |
| 65 | unverified | match | 0.702/1.905/5,224.1 | 0.476/1.070/5,798.8 | —/—/— |
| 66 | unverified | match | 0.847/1.970/5,255.2 | 0.702/1.290/5,992.4 | —/—/— |
| 67 | unverified | match | 2.228/6.595/5,050.4 | 0.872/2.520/6,060.4 | —/—/— |
| 68 | unverified | match | 0.424/0.960/4,879.5 | 0.339/0.695/5,979.8 | —/—/— |
| 69 | unverified | match | 0.978/2.170/4,945.7 | 0.615/1.015/6,227.5 | —/—/— |
| 70 | unverified | match | 0.606/1.495/4,936.9 | 0.421/0.710/6,247.1 | —/—/— |
| 71 | unverified | match | 0.558/1.195/4,963.3 | 0.355/0.610/6,247.3 | —/—/— |
| 72 | unverified | match | 4.739/15.415/6,424.6 | 9.022/30.585/7,131.3 | —/—/— |
| 73 | unverified | match | 0.439/1.170/6,508.9 | 0.286/0.510/7,066.9 | —/—/— |
| 74 | unverified | match | 1.229/3.070/8,137.7 | 0.768/1.540/6,962.6 | —/—/— |
| 75 | unverified | match | 1.576/3.780/8,136.8 | 1.042/1.945/6,843.6 | —/—/— |
| 76 | unverified | match | 0.498/1.045/8,571.1 | 0.358/0.520/6,843.8 | —/—/— |
| 77 | unverified | values differ | 0.822/1.750/8,619.6 | —/—/— | —/—/— |
| 78 | unverified | match | 1.582/4.500/8,623.1 | 0.812/1.960/6,782.9 | —/—/— |
| 79 | unverified | match | 0.436/0.950/8,625.5 | 0.302/0.595/6,727.2 | —/—/— |
| 80 | unverified | match | 1.309/3.065/8,627.2 | 0.867/1.770/6,755.1 | —/—/— |
| 81 | unverified | match | 0.484/1.075/8,628.3 | 0.378/0.700/6,738.8 | —/—/— |
| 82 | unverified | match | 0.721/1.595/8,628.8 | 0.408/0.885/6,857.2 | —/—/— |
| 83 | unverified | match | 0.574/0.970/8,629.6 | 0.486/0.675/6,794.7 | —/—/— |
| 84 | unverified | match | 0.482/0.925/8,630.2 | 0.272/0.460/6,716.3 | —/—/— |
| 85 | unverified | match | 0.900/2.180/8,631.4 | 0.599/1.335/6,706.6 | —/—/— |
| 86 | unverified | match | 0.256/0.520/8,632.2 | 0.218/0.400/6,706.8 | —/—/— |
| 87 | unverified | match | 0.755/1.795/8,633.1 | 0.504/0.805/6,707.0 | —/—/— |
| 88 | unverified | match | 1.382/3.420/8,827.5 | 1.122/2.340/6,646.0 | —/—/— |
| 89 | unverified | match | 0.363/0.735/9,027.2 | 0.245/0.415/6,584.9 | —/—/— |
| 90 | unverified | match | 0.328/0.525/9,027.7 | 0.257/0.350/6,585.1 | —/—/— |
| 91 | unverified | match | 0.451/0.805/9,028.4 | 0.343/0.545/6,585.7 | —/—/— |
| 92 | unverified | match | 0.321/0.575/9,028.8 | 0.238/0.340/6,585.9 | —/—/— |
| 93 | unverified | match | 0.440/0.900/9,029.2 | 0.240/0.420/6,585.9 | —/—/— |
| 94 | unverified | match | 0.669/1.690/8,847.4 | 0.368/0.840/6,592.2 | —/—/— |
| 95 | unverified | match | 1.130/3.105/8,617.3 | 0.576/1.280/6,596.6 | —/—/— |
| 96 | unverified | match | 0.229/0.475/8,617.3 | 0.188/0.395/6,596.8 | —/—/— |
| 97 | unverified | match | 0.689/1.835/8,617.3 | 0.359/0.890/6,597.0 | —/—/— |
| 98 | unverified | match | 0.392/0.955/8,617.4 | 0.277/0.520/6,549.8 | —/—/— |
| 99 | unverified | match | 0.331/0.550/8,617.4 | 0.271/0.465/6,550.0 | —/—/— |

The reports cover the 99 SF1 query files and compare the rows returned by each statement. Cases without a validated warm timing have an empty timing cell. Larger scale factors were outside this run.
## Anomalies and log excerpts

```text
sail | IllegalArgumentException: invalid argument: found polaris at 4:11 expected 'DATABASE', 'SCHEMA', 'NAMESPACE', or 'CATALOG'
```

[Raw JSON](https://github.com/unikdahal/poc/blob/22e97bc92271747cff8303c1a39ae450a0fed464/agent2-tpcds/tpcds.json) · [Result files](https://github.com/unikdahal/poc/tree/22e97bc92271747cff8303c1a39ae450a0fed464/agent2-tpcds) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45493612852)
