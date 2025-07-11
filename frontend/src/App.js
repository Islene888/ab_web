import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import ExperimentPage from './page/ExperimentPage';
import AbTestResultPage from './page/AbTestResultPage.jsx';
import './styles/dark-input.css';
import './styles/form-bar.css';


function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<ExperimentPage />} />
        <Route path="/abtest" element={<AbTestResultPage />} />
      </Routes>
    </Router>
  );
}

export default App;
