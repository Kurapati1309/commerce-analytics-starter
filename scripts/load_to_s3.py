import argparse, os, boto3, pathlib

def sync_dir_to_s3(local_dir, bucket):
    s3 = boto3.client("s3")
    for root, _, files in os.walk(local_dir):
        for f in files:
            path = os.path.join(root, f)
            key = os.path.relpath(path, local_dir).replace('\\','/')
            s3.upload_file(path, bucket, key)
            print("Uploaded", key)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--local_dir", default="scripts/data")
    args = ap.parse_args()
    sync_dir_to_s3(args.local_dir, args.bucket)
