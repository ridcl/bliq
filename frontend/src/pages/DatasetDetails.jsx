import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import * as arrow from 'apache-arrow';
import './DatasetDetails.css';

function DatasetDetails() {
  const { id } = useParams(); // This is the encoded dataset name (e.g., "test/employees/v1")
  const datasetName = decodeURIComponent(id);

  const [description, setDescription] = useState(null);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [loadingData, setLoadingData] = useState(false);

  useEffect(() => {
    fetchDescription();
  }, [datasetName]);

  const fetchDescription = async () => {
    try {
      setLoading(true);
      const response = await fetch(
        `http://localhost:8000/api/v1/datasets/describe?name=${encodeURIComponent(datasetName)}`
      );

      if (!response.ok) {
        throw new Error('Dataset not found');
      }

      const descText = await response.text();
      setDescription(descText);
      setError(null);

      // Also load sample data
      await fetchSampleData();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const fetchSampleData = async (rowLimit = 20) => {
    try {
      setLoadingData(true);
      const response = await fetch(
        `http://localhost:8000/api/v1/datasets/load?name=${encodeURIComponent(datasetName)}&limit=${rowLimit}`
      );

      if (!response.ok) {
        throw new Error('Failed to load dataset');
      }

      // Read Arrow IPC stream
      const arrayBuffer = await response.arrayBuffer();
      const table = arrow.tableFromIPC(arrayBuffer);

      // Convert to JSON for display
      const rows = [];
      for (let i = 0; i < table.numRows; i++) {
        const row = {};
        for (const field of table.schema.fields) {
          const column = table.getChild(field.name);
          row[field.name] = column.get(i);
        }
        rows.push(row);
      }

      setData({
        schema: table.schema.fields.map(f => ({ name: f.name, type: f.type.toString() })),
        rows: rows,
        totalRows: table.numRows,
      });
    } catch (err) {
      console.error('Failed to load sample data:', err);
      setData({ error: err.message });
    } finally {
      setLoadingData(false);
    }
  };

  if (loading) {
    return (
      <div className="container">
        <div className="loading">Loading dataset details...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container">
        <div className="error">Error: {error}</div>
        <Link to="/" className="back-button">
          ← Back to List
        </Link>
      </div>
    );
  }

  const tableColumns = data && data.rows && data.rows.length > 0 ? Object.keys(data.rows[0]) : [];

  return (
    <div className="container">
      <Link to="/" className="back-link">
        ← Back to List
      </Link>

      <div className="dataset-header">
        <h1 className="dataset-title">{datasetName}</h1>

        {description && (
          <div className="dataset-description">
            <h2>Description</h2>
            <pre className="description-text">{description}</pre>
          </div>
        )}
      </div>

      <div className="data-preview">
        <h2>Data Preview (First 20 Rows)</h2>

        {loadingData ? (
          <p className="loading">Loading data...</p>
        ) : data && data.error ? (
          <p className="error">Failed to load data: {data.error}</p>
        ) : !data || !data.rows || data.rows.length === 0 ? (
          <p className="no-data">No data available</p>
        ) : (
          <>
            <div className="data-info">
              Showing {data.rows.length} row{data.rows.length !== 1 ? 's' : ''}
            </div>
            <div className="table-container">
              <table className="data-table">
                <thead>
                  <tr>
                    <th className="row-number">#</th>
                    {tableColumns.map(column => (
                      <th key={column}>{column}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.rows.map((row, index) => (
                    <tr key={index}>
                      <td className="row-number">{index + 1}</td>
                      {tableColumns.map(column => (
                        <td key={column}>
                          {formatValue(row[column])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function formatValue(value) {
  if (value === null || value === undefined) {
    return <span className="null-value">null</span>;
  }

  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }

  if (typeof value === 'number') {
    return value.toLocaleString();
  }

  if (value instanceof Date || (typeof value === 'object' && value.constructor.name.includes('Timestamp'))) {
    try {
      return new Date(value).toLocaleString();
    } catch {
      return String(value);
    }
  }

  if (typeof value === 'object') {
    return JSON.stringify(value);
  }

  return String(value);
}

export default DatasetDetails;
