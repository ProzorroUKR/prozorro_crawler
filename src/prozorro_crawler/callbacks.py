def warn_db_conflicts_base(enabled, key, value, default):
    from prozorro_crawler.settings import logger
    if enabled and value == default:
        logger.warning(
            f"Environment variable {key} "
            f"casted with default value '{default}'. "
            f"This may cause conflicts if you use "
            f"one db for many crawlers, "
            f"better rename to a specific process, "
            f"in order different crawlers don't clash."
        )


def warn_db_conflicts(key, value, default):
    warn_db_conflicts_base(True, key, value, default)


def warn_mongodb_conflicts(key, value, default):
    from prozorro_crawler.settings import MONGODB_URL
    warn_db_conflicts_base(MONGODB_URL, key, value, default)


def warn_postgres_conflicts(key, value, default):
    from prozorro_crawler.settings import POSTGRES_HOST
    warn_db_conflicts_base(POSTGRES_HOST, key, value, default)


def warn_crawler_user_agent(key, value, default):
    from prozorro_crawler.settings import logger
    if value == default:
        logger.warning(
            f"Using default '{value}' as crawler user agent. "
            f"To set another user agent set {key} environment variable."
        )
    else:
        logger.info(
            f"Using '{value}' as crawler user agent."
        )


def assert_url(key, value, default):
    assert not value.endswith("/")
    assert value.startswith("http")
