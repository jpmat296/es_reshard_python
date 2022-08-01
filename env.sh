#
# Usage: source env.sh
#
export ELASTICSEARCH_HOSTS=$(op get item "Elastic matsusoft" --fields host)
export ELASTICSEARCH_USERNAME=$(op get item "Elastic matsusoft" --fields username)
export ELASTICSEARCH_PASSWORD=$(op get item "Elastic matsusoft" --fields password)
