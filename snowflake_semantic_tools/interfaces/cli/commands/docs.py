"""
Docs Command

CLI commands for generating and serving interactive documentation
for the semantic layer. Produces a self-contained HTML site with
lineage visualization and a searchable component catalog.
"""

import http.server
import json
import logging
import socketserver
import threading
import time
import webbrowser
from pathlib import Path
from typing import Any, Optional

import click

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput
from snowflake_semantic_tools.shared.events import setup_events


def _setup(verbose: bool, quiet: bool) -> None:
    setup_events(verbose=verbose, quiet=quiet, show_timestamps=True)
    if verbose:
        logging.getLogger("snowflake_semantic_tools").setLevel(logging.DEBUG)
    elif quiet:
        logging.getLogger("snowflake_semantic_tools").setLevel(logging.ERROR)


def _ensure_manifest(output: CLIOutput) -> Optional[Path]:
    from snowflake_semantic_tools.services.compile import MANIFEST_FILENAME

    manifest_path = Path("target") / MANIFEST_FILENAME
    if manifest_path.exists():
        return manifest_path

    output.info("No compiled manifest found — running 'sst compile' automatically...")
    try:
        from snowflake_semantic_tools.services.compile import CompileConfig, CompileService

        service = CompileService()
        result = service.compile(CompileConfig())
        if result.success:
            output.success("Compile", duration=result.duration)
            return result.manifest_path
        else:
            for err in result.errors:
                output.error(err)
            return None
    except Exception as e:
        output.error(f"Auto-compile failed: {e}")
        output.info("Run 'sst compile' manually to troubleshoot.")
        return None


@click.group("docs", invoke_without_command=True, short_help="Generate and serve semantic model documentation")
@click.pass_context
def docs_cmd(ctx: click.Context) -> None:
    """Generate and serve interactive documentation for your semantic layer.

    \b
    Produces a self-contained HTML site with:
      - Searchable component catalog (tables, metrics, relationships, ...)
      - Interactive lineage graph with D3.js
      - JSON data export

    \b
    Quick Start:
      sst docs                         Generate + serve (default)
      sst docs generate                Generate static site only
      sst docs generate --format json  Export catalog as JSON
      sst docs serve                   Serve existing docs site

    \b
    The site is fully self-contained — no internet connection needed.
    Host on GitHub Pages, S3, or any static file server.
    """
    if ctx.invoked_subcommand is None:
        ctx.invoke(generate_cmd)
        output_dir = ctx.obj.get("docs_output_dir", Path("sst-docs"))
        if output_dir.exists():
            ctx.invoke(serve_cmd, directory=str(output_dir))


@docs_cmd.command("generate")
@click.option("--output", "-o", "output_dir", default="sst-docs", help="Output directory for generated docs")
@click.option(
    "--format", "-f", "output_format", type=click.Choice(["html", "json"]), default="html", help="Output format"
)
@click.option("--verbose", "-v", is_flag=True, help="Show debug output")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-essential output")
@click.pass_context
def generate_cmd(ctx: click.Context, output_dir: str, output_format: str, verbose: bool, quiet: bool) -> None:
    """Generate a static documentation site from the compiled manifest.

    \b
    Examples:
      sst docs generate                        Default HTML site in ./sst-docs/
      sst docs generate --output ./my-docs     Custom output directory
      sst docs generate --format json          JSON export only
    """
    _setup(verbose, quiet)
    output = CLIOutput(verbose=verbose, quiet=quiet)

    if not quiet:
        output.info(f"Running with sst={__version__}")
        output.blank_line()
        output.header("Generating documentation...")
        output.rule()

    start = time.time()

    manifest_path = _ensure_manifest(output)
    if not manifest_path:
        output.error("Cannot generate documentation without a compiled manifest.")
        output.info("Ensure you have a valid dbt project with SST annotations.")
        raise SystemExit(1)

    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest_data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        output.error(f"Could not read manifest at {manifest_path}: {e}")
        raise SystemExit(1)

    tables_data = manifest_data.get("tables", {})
    tables_count = len(tables_data.get("tables", []))
    metrics_count = len(tables_data.get("metrics", []))
    rels_count = len(tables_data.get("relationships", []))
    views_count = len(tables_data.get("semantic_views", []))
    filters_count = len(tables_data.get("filters", []))
    vq_count = len(tables_data.get("verified_queries", []))

    if not quiet:
        output.info(f"  {tables_count} tables, {metrics_count} metrics, {rels_count} relationships")
        output.info(f"  {views_count} semantic views, {filters_count} filters, {vq_count} verified queries")

    from snowflake_semantic_tools.services.lineage_builder import LineageGraphBuilder

    graph = LineageGraphBuilder.from_manifest_data(manifest_data)

    if not quiet:
        output.info(f"  Lineage: {len(graph.nodes)} nodes, {len(graph.edges)} edges")

    from snowflake_semantic_tools.services.docs_generator import DocsConfig, DocsGenerator

    out_path = Path(output_dir)
    generator = DocsGenerator(manifest_data, graph)
    result = generator.generate(DocsConfig(output_dir=out_path, format=output_format))

    duration = time.time() - start

    if not result.success:
        for err in result.errors:
            output.error(err)
        raise SystemExit(1)

    if not quiet:
        output.rule()
        for created_file in result.files_created:
            output.info(f"  Created {created_file}")
        output.blank_line()
        output.success("Documentation generated", duration=duration)
        output.blank_line()
        if output_format == "html":
            click.echo(f"  To serve locally:  sst docs serve --dir {output_dir}")
            click.echo(f"  To deploy:         Upload {output_dir}/ to any static host")

    ctx.ensure_object(dict)
    ctx.obj["docs_output_dir"] = out_path


@docs_cmd.command("serve")
@click.option("--port", "-p", default=8000, type=int, help="Port to serve on")
@click.option("--dir", "-d", "directory", default="sst-docs", help="Directory to serve")
@click.option("--open/--no-open", "open_browser", default=True, help="Open browser automatically")
@click.option("--verbose", "-v", is_flag=True, help="Show debug output")
@click.option("--quiet", "-q", is_flag=True, help="Suppress non-essential output")
def serve_cmd(port: int, directory: str, open_browser: bool, verbose: bool, quiet: bool) -> None:
    """Serve the documentation site locally.

    \b
    Examples:
      sst docs serve                     Serve on default port 8000
      sst docs serve --port 3000         Custom port
      sst docs serve --no-open           Don't open browser
      sst docs serve --dir ./my-docs     Serve from custom directory
    """
    _setup(verbose, quiet)
    output = CLIOutput(verbose=verbose, quiet=quiet)

    serve_path = Path(directory)
    if not serve_path.exists():
        output.error(f"Directory not found: {serve_path}")
        output.info("Run 'sst docs generate' first to create the documentation site.")
        raise SystemExit(1)

    index_path = serve_path / "index.html"
    if not index_path.exists():
        output.error(f"No index.html found in {serve_path}")
        output.info("Run 'sst docs generate' to create documentation files.")
        raise SystemExit(1)

    url = f"http://localhost:{port}"

    if not quiet:
        output.blank_line()
        output.header("SST Documentation Server")
        output.rule()
        click.echo(f"  Local:   {url}")
        click.echo(f"  Source:  {serve_path.resolve()}")
        click.echo("")
        click.echo("  Press Ctrl+C to stop the server")
        output.rule()

    if open_browser:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["directory"] = str(serve_path.resolve())
            super().__init__(*args, **kwargs)

        def log_message(self, format: str, *args: Any) -> None:
            if verbose:
                super().log_message(format, *args)

    try:
        with socketserver.TCPServer(("", port), QuietHandler) as httpd:
            httpd.serve_forever()
    except KeyboardInterrupt:
        if not quiet:
            click.echo("\n  Server stopped.")
    except OSError as e:
        output.error(f"Could not start server on port {port}: {e}")
        output.info(f"Try a different port: sst docs serve --port {port + 1}")
        raise SystemExit(1)
