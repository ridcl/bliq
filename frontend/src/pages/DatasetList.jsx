import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import './DatasetList.css';

function DatasetList() {
  const [datasets, setDatasets] = useState([]);
  const [filteredDatasets, setFilteredDatasets] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchDatasets();
  }, []);

  useEffect(() => {
    filterDatasets();
  }, [searchQuery, datasets]);

  const fetchDatasets = async () => {
    try {
      setLoading(true);

      // For now, we'll use a placeholder list since the API doesn't have a list endpoint yet
      // In production, this would call /api/v1/datasets/list

      // Hardcoded example datasets that might exist
      const exampleDatasets = [
        { name: 'test/employees/v1', namespace: 'test', dataset: 'employees', version: 'v1' },
        { name: 'analytics/users/v1', namespace: 'analytics', dataset: 'users', version: 'v1' },
        { name: 'analytics/users/v2', namespace: 'analytics', dataset: 'users', version: 'v2' },
      ];

      setDatasets(exampleDatasets);
      setFilteredDatasets(exampleDatasets);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const filterDatasets = () => {
    if (!searchQuery.trim()) {
      setFilteredDatasets(datasets);
      return;
    }

    const query = searchQuery.toLowerCase();
    const filtered = datasets.filter(dataset =>
      dataset.name.toLowerCase().includes(query)
    );
    setFilteredDatasets(filtered);
  };

  if (loading) {
    return (
      <div className="container">
        <div className="loading">Loading datasets...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container">
        <div className="error">Error: {error}</div>
        <button onClick={fetchDatasets} className="retry-button">
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="container">
      <header className="header">
        <h1>Bliq Dataset Catalog</h1>
        <p className="subtitle">Versioned dataset management with block storage</p>
      </header>

      <div className="search-bar">
        <input
          type="text"
          placeholder="Search datasets by name..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="search-input"
        />
        {searchQuery && (
          <button
            onClick={() => setSearchQuery('')}
            className="clear-button"
          >
            Clear
          </button>
        )}
      </div>

      <div className="results-info">
        {filteredDatasets.length} dataset{filteredDatasets.length !== 1 ? 's' : ''} found
      </div>

      <div className="dataset-grid">
        {filteredDatasets.length === 0 ? (
          <div className="no-results">
            No datasets found matching "{searchQuery}"
          </div>
        ) : (
          filteredDatasets.map(dataset => (
            <Link
              to={`/dataset/${encodeURIComponent(dataset.name)}`}
              key={dataset.name}
              className="dataset-card"
            >
              <h3 className="dataset-name">{dataset.dataset}</h3>
              <p className="dataset-namespace">
                <span className="label">Namespace:</span> {dataset.namespace}
              </p>
              <p className="dataset-version">
                <span className="label">Version:</span> {dataset.version}
              </p>
              <div className="view-details">View Details →</div>
            </Link>
          ))
        )}
      </div>
    </div>
  );
}

export default DatasetList;
