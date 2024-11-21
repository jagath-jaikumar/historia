from invoke import task, Collection


@task
def docker(ctx):
    """Build and run the Docker containers"""
    with ctx.cd("deploy"):
        ctx.run("docker compose build")
        ctx.run("docker compose up")


@task
def migrate(ctx):
    """Run database migrations in the Docker container"""
    with ctx.cd("deploy"):
        ctx.run("docker compose exec web python -m historia.manage migrate")


@task
def reset_db(ctx):
    """Reset the database to an empty state"""
    with ctx.cd("deploy"):
        # Stop the database service
        ctx.run("docker compose stop db")

        # Remove the database volume to delete all data
        ctx.run("docker compose down -v")

        # Recreate and start the database service
        ctx.run("docker compose up -d db")

        # Wait for the database to become healthy
        print("Waiting for the database to be ready...")
        ctx.run("docker compose exec db pg_isready -U historia")
        print("Database reset complete. You can now re-run migrations.")


# Create local namespace
local = Collection("local")
local.add_task(docker, name="docker")
local.add_task(migrate, name="migrate")
local.add_task(reset_db, name="reset-db")

# Add 'local' to the root namespace
ns = Collection()
ns.add_collection(local)
