import React, {useState} from 'react';
import axios  from 'axios';
import './App.css';

function App() {
  const [start, setStartDate] = useState(new Date());
  const [end, setEndDate] = useState(new Date());
  const [analytics, setAnalytics] = useState({});

  const getAnalytics = async () => {
    const params = {
      start: formatDate(new Date(start)),
      end: formatDate(new Date(end))
    }
    
    const {data} = await axios.get("http://localhost:8080/getAnalytics", {params})
    console.log(data);
    setAnalytics(data);
  }

  const formatDate = (dt) => {
    const y = dt.getFullYear();
    const m = dt.getMonth() +1;
    const d = dt.getDate();
    const h = dt.getHours();
    const min = dt.getMinutes();
    return y+'-'+m+'-'+d+' '+h+':'+min+':00';
  }


  return (
    <div className="App">
      <div className='header'>Health Monitor</div>
      <div className='controls'>
        <label>
          Start Date
          <input className='date-picker' type="datetime-local" onChange={(e) => setStartDate(e.target.value)}/>
        </label>
        <label>
          End Date
          <input className='date-picker' type="datetime-local" onChange={(e) => setEndDate(e.target.value)}/>
        </label>
        <div className='show-btn' onClick={getAnalytics}>Show Analytics</div>
      </div>
      <div className='data-show'>
        <table className='data-table'>
          <tbody>
            <tr>
              <th>Service Name</th>
              <th>Mean CPU</th>
              <th>CPU peak time</th>
              <th>Mean RAM</th>
              <th>RAM peak time </th>
              <th>Mean Disk</th>
              <th>Disk peak time</th>
              <th># messages recieved</th>
            </tr>

            {Object.entries(analytics).map(([serviceName, props]) => {
              const parts = props.split(" ");
              return(
                <tr key={serviceName}>
                  <td>{serviceName}</td>
                  <td>{parseInt(parts[0] * 100) + " %"}</td>
                  <td>{parts[1] + " | " + parts[2]}</td>
                  <td>{parseInt(parts[3] * 100) + " %"}</td>
                  <td>{parts[4] + " | " + parts[5]}</td>
                  <td>{parseInt(parts[6] * 100) + " %"}</td>
                  <td>{parts[7] + " | " + parts[8]}</td>
                  <td>{parts[9]}</td>
                </tr>);
              })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default App;
