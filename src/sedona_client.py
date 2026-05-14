# geo_agent/sedona_client.py
import os
from sedona.spark import SedonaContext

_sedona = None


def get_sedona():
    global _sedona
    if _sedona is not None:
        return _sedona

    # Resolve profile into env vars so Spark's JVM can use them (no-op if already set, e.g. CI)
    from src.cloud_auth import ensure_aws_credentials_from_profile
    ensure_aws_credentials_from_profile()

    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", (
        "--packages "
        "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.8.1,"
        "org.datasyslab:geotools-wrapper:1.8.1-33.1,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
        "pyspark-shell"
    ))

    builder = (
        SedonaContext.builder()
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.jars.ivy", os.path.expanduser("~/.ivy2"))
        .config("spark.jars.packages.excludeTransitiveDependencies", "false")
        # Anonymous access for public S3 (e.g. Overture Maps reads through Spark)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    )

    # If S3_ENRICHED_BASE is set, configure authenticated access for that bucket.
    # Extract bucket name from the URI (e.g. "s3a://my-bucket/path" → "my-bucket").
    s3_base = os.environ.get("S3_ENRICHED_BASE", "").strip()
    if s3_base:
        try:
            bucket = s3_base.split("://", 1)[1].split("/")[0]
            if bucket:
                builder = builder.config(
                    f"spark.hadoop.fs.s3a.bucket.{bucket}.aws.credentials.provider",
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider,"
                    "com.amazonaws.auth.profile.ProfileCredentialsProvider",
                )
        except IndexError:
            pass

    _sedona = SedonaContext.create(builder.getOrCreate())
    _sedona.sparkContext.setLogLevel("ERROR")
    return _sedona
    

def ensure_region_loaded(region: str = "sydney"):
    """
    Called once at app startup — not by the agent.
    Downloads data if missing, loads into Spark, caches view.
    """
    global _loaded_regions
    if region in _loaded_regions:
        return

    cfg = REGION_CONFIGS[region]
    sd = get_sedona()

    # Download only if needed
    if not os.path.exists(cfg["file"]):
        os.makedirs("data", exist_ok=True)
        subprocess.run([
            "overturemaps", "download",
            f"--bbox={cfg['bbox']}",
            "-f", "geoparquet", "--type=building",
            "-o", cfg["file"],
        ], check=True)
    # Load + cache — agent never touches this
    df = sd.read.format("geoparquet").load(cfg["file"])
    df.cache()
    df.createOrReplaceTempView(cfg["view"])

    _loaded_regions.add(region)
