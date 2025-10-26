import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import DatasetList from './pages/DatasetList';
import DatasetDetails from './pages/DatasetDetails';
import './App.css';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<DatasetList />} />
        <Route path="/dataset/:id" element={<DatasetDetails />} />
      </Routes>
    </Router>
  );
}

export default App;
