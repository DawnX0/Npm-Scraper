from itemadapter import ItemAdapter
from io import BytesIO
from dotenv import load_dotenv
from psycopg2.extras import Json
import psycopg2
import aiohttp
import tarfile
import os
import asyncio
import json
import jsbeautifier

load_dotenv()

class NpmPipeline:
    def __init__(self):
        self.session = None

    def open_spider(self, spider):
        loop = asyncio.get_event_loop()
        self.session = aiohttp.ClientSession(loop=loop)

    def close_spider(self, spider):
        if self.session:
            loop = asyncio.get_event_loop()
            loop.create_task(self.session.close())

    async def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        url = adapter.get("url")

        if self.session is None:
            self.session = aiohttp.ClientSession()

        try:
            async with self.session.get(url) as resp:
                data = await resp.json(content_type=None)

            if not data:
                print(f"{url} returned empty JSON")
                return item

            versions = data.get("versions")
            if not versions:
                print(f"{url} has no versions")
                return item

            first_version = next(iter(versions))
            version_info = versions.get(first_version)
            if not version_info:
                print(f"{url} first version missing info")
                return item

            dist_info = version_info.get("dist")
            if not dist_info:
                print(f"{url} first version missing dist info")
                return item

            tar_url = dist_info.get("tarball")
            if not tar_url:
                print(f"{url} first version has no tarball")
                return item

            author_info = version_info.get("author")
            if isinstance(author_info, dict):
                item["author"] = author_info.get("name")
            elif isinstance(author_info, str):
                item["author"] = author_info
            else:
                item["author"] = None

            license_info = version_info.get("license")
            if isinstance(license_info, dict):
                item["license"] = license_info.get("type")
            elif isinstance(license_info, str):
                item["license"] = license_info
            else:
                item["license"] = None

            item["name"] = data.get("name", "unknown")
            item["version"] = data.get("dist-tags", {}).get("latest", "unknown")

            print("[STATUS]: Opening tar url....")
            async with self.session.get(tar_url) as tar_resp:
                print("[STATUS]: Saving tar bytes....")
                tar_bytes = await tar_resp.read()
                item["tar_bytes"] = tar_bytes
                print("[STATUS]: Saved tar bytes!")

        except Exception as e:
            print(f"[ERROR] {url} -> {e}")

        return item


class PostgresPipeline:
    def __init__(self):
        self.dsn = os.getenv("POSTGRES_DSN")

    def open_spider(self, spider):
        print("[STATUS] Opening Postgres connection!")
        self.conn = psycopg2.connect(dsn=self.dsn)
        self.cur = self.conn.cursor()
        print("[STATUS] Postgres connection opened!")
        self.ensure_schema()

    def close_spider(self, spider):
        print("[STATUS] Closing Postgres connection!")
        self.cur.close()
        self.conn.close()
        print("[STATUS] Postgres connection closed!")

    def ensure_schema(self):
        print("[STATUS] Ensuring schema...")
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS package_registry (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                version TEXT NOT NULL,
                author TEXT,
                license TEXT,
                metadata JSONB,
                files JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
        )
        self.conn.commit()
        print("[STATUS] Schema ensured!")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        name = adapter.get("name")
        version = adapter.get("version")
        author = adapter.get("author")
        license = adapter.get("license")
        tar_bytes = adapter.get("tar_bytes")

        # Extract key files from tar
        files_content = self.extract_files(tar_bytes)

        # Store package.json prettified if exists
        metadata = {}
        if "package.json" in files_content:
            try:
                metadata = json.loads(files_content["package.json"])
            except json.JSONDecodeError:
                metadata = {}

        # Prettify JSON before saving
        metadata_str = json.dumps(metadata, indent=2)
        files_json_str = json.dumps(files_content, indent=2)

        self.cur.execute(
            """
            INSERT INTO package_registry (name, version, author, license, metadata, files)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (name, version, author, license, Json(metadata), Json(files_content))
        )
        self.conn.commit()
        print(f"[LOG] Saved package: {name}@{version}")
        return item

    def extract_files(self, tar_bytes):
        """
        Extract key files from tar content, prettify JS/TS and JSON, and clean filenames/content.
        """
        files_content = {}
        tar_file = BytesIO(tar_bytes)
        opts = jsbeautifier.default_options()
        opts.indent_size = 2

        try:
            with tarfile.open(fileobj=tar_file, mode="r:gz") as tar:
                for member in tar.getmembers():
                    if member.isfile() and member.name.endswith((".js", ".ts", "README.md", "package.json")):
                        f = tar.extractfile(member)
                        if f:
                            content = f.read()
                            # Convert bytes to string
                            content = content.decode("utf-8", errors="ignore")
                            # Prettify JS/TS
                            if member.name.endswith((".js", ".ts")):
                                content = jsbeautifier.beautify(content, opts)
                            # Prettify package.json
                            if os.path.basename(member.name) == "package.json":
                                try:
                                    json_obj = json.loads(content)
                                    content = json.dumps(json_obj, indent=2)
                                except json.JSONDecodeError:
                                    pass
                            # Remove slashes from filename and content
                            clean_name = os.path.basename(member.name).replace("/", "_")
                            content = content.replace("/", "_")
                            files_content[clean_name] = content
        except tarfile.ReadError:
            print("[WARN] Could not read tarball, skipping")

        return files_content