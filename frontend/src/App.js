import React, { useEffect, useRef, useState } from 'react';

import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import '@mapbox/mapbox-gl-geocoder/dist/mapbox-gl-geocoder.css';
import MapboxGeocoder from '@mapbox/mapbox-gl-geocoder';
import { featureCollection } from '@turf/turf';
import axios from 'axios';

import './App.css';

const MAPBOX_API_KEY = "pk.eyJ1IjoidGhhbmhoYWlpMDMiLCJhIjoiY2xwZ3R0dTJpMDFmODJxbGZpMTB0bG93dCJ9.zPguGUAukU-bfVTDnW-NlQ"
mapboxgl.accessToken = MAPBOX_API_KEY;


function App() {
    const map = useRef(null);
    const mapContainer = useRef(null);
    const mapClick = useRef(null);
    const [markedDone, setMarkedDone] = useState(false);
    const [point, setPoint] = useState(featureCollection([]));
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        if (map.current) return;
        map.current = new mapboxgl.Map({
            container: mapContainer.current,
            style: 'mapbox://styles/mapbox/streets-v12',
            center: [105.804437379, 20.9959311226],
            zoom: 16,
        });
        map.current.on('load', async () => {
        });

        if (!markedDone) {
            mapClick.current = handleMapClick;
            map.current.on('click', mapClick.current);
        }

        const geocoder = new MapboxGeocoder({
            accessToken: mapboxgl.accessToken,
            mapboxgl: mapboxgl,
            placeholder: 'Search...',
            // bbox: [103, 20.53, 106.02, 21.23],
            countries: 'VN',
            language: 'vi-VN',
            enableHighAccuracy: true,
        });

        map.current.addControl(geocoder, 'top-right');

        const navigationControl = new mapboxgl.NavigationControl();
        map.current.addControl(navigationControl);

        const geolocateControl = new mapboxgl.GeolocateControl({
            positionOptions: {
                enableHighAccuracy: true,
            },
            trackUserLocation: true,
        });
        map.current.addControl(geolocateControl);

    });

    // choose position by click
    function handleMapClick(e) {
        handleReset();
        setMarkedDone(true);

        // Cập nhật toạ độ trong state
        setPoint(featureCollection([
            {
                type: 'Feature',
                geometry: {
                    type: 'Point',
                    coordinates: [e.lngLat.lng, e.lngLat.lat]
                }
            }
        ]));

        map.current.addLayer({
            id: `dropoff-`,
            type: 'circle',
            source: {
                data: {
                    type: 'FeatureCollection',
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'Point',
                                coordinates: [e.lngLat.lng, e.lngLat.lat]
                            }
                        }
                    ]
                },
                type: 'geojson'
            },

            paint: {
                'circle-radius': 10,
                'circle-color': '#3081D0',
            },
        });

        map.current.addLayer({
            id: `dropoff-symbol`,
            type: 'symbol',
            source: {
                data: {
                    type: 'FeatureCollection',
                    features: [
                        {
                            type: 'Feature',
                            geometry: {
                                type: 'Point',
                                coordinates: [e.lngLat.lng, e.lngLat.lat]
                            }
                        }
                    ]
                },
                type: 'geojson'
            },
            layout: {
                'icon-size': 0.5,
                'text-field': `${e.lngLat.lat}, ${e.lngLat.lng}`,
                'text-font': ['Arial Unicode MS Bold'],
                'text-offset': [0, 0.9],
                'text-anchor': 'top',
                'text-size': 10
            },
        });
        point.features.push({
            type: 'Feature',
            geometry: {
                type: 'Point',
                coordinates: [e.lngLat.lng, e.lngLat.lat]
            }
        });

        // Gửi toạ độ lên server 
        const coordinatesData = {
            lat: e.lngLat.lat,
            lon: e.lngLat.lng
        };

        setLoading(true);
        axios.post('http://localhost:8000/aqi', coordinatesData, { responseType: 'json' })
            .then(response => {
                console.log(response.data);

                const responseData = response.data;
                const result = responseData;

                setResults(result);
                setLoading(false);
            })
            .catch(error => {
                console.error(error);
                setLoading(false);
            });
    };

    // reset
    function handleReset() {
        if (map.current.getLayer(`dropoff-`)) {
            map.current.removeLayer(`dropoff-`);
        }
        if (map.current.getSource(`dropoff-`)) {
            map.current.removeSource(`dropoff-`);
        }
        if (map.current.getLayer(`dropoff-symbol`)) {
            map.current.removeLayer(`dropoff-symbol`);
        }
        if (map.current.getSource(`dropoff-symbol`)) {
            map.current.removeSource(`dropoff-symbol`);
        }

        point.features = [];
        setMarkedDone(false);
        setResults([]);

        if (mapClick.current) {
            map.current.off('click', mapClick.current);
        }
        mapClick.current = handleMapClick;
        map.current.on('click', handleMapClick);

        console.log("Reset done");
    }

    function labelResult(item) {
        let predictionClass = '';
        if (item <= 50) {
            predictionClass = 'good';
        } else if (item <= 100) {
            predictionClass = 'moderate';
        } else if (item <= 150) {
            predictionClass = 'unhealthy-for-sg';
        } else if (item <= 200) {
            predictionClass = 'unhealthy';
        } else if (item <= 300) {
            predictionClass = 'very-unhealthy';
        } else {
            predictionClass = 'hazardous';
        }
        return predictionClass;
    }


    return (
        <div>
            <div className="sidebar">
                <h1 style={{ fontSize: 40, textAlign: "center" }}>Air Quality Prediction</h1>

                <div className="image-logo">
                    <img src='static/aqi.jpg' alt="aqi" width={600} />
                </div>

                <p style={{ textAlign: "center", fontSize: 20 }}>Click on the map to choose the location</p>

                {markedDone && (
                    <div>
                        <h2>Coordinates:</h2>
                        <div align='center'>
                            {point.features.length > 0 && (
                                <span>
                                    {point.features[0].geometry.coordinates[1]}, {point.features[0].geometry.coordinates[0]}
                                </span>
                            )}
                        </div>

                        <div>
                            <h2>Air Quality Prediction For Next 7 Days: </h2>

                            {
                                loading ? (
                                    <div className='loading-spinner' align='center' />
                                ) : (
                                    <div className='display-result'>
                                        <table align='center'>
                                            <thead>
                                                <tr>
                                                    <th>Date</th>
                                                    <th>Time</th>
                                                    <th>AQI</th>
                                                    <th>Air Quality</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {results.map((doc, index) => {
                                                    const [date, time] = doc.date.split("T")
                                                    let previousDate = '';

                                                    if (index > 0) {
                                                        previousDate = results[index - 1].date.split("T")[0];
                                                    }

                                                    let predictionClass = labelResult(doc.AQI_prediction);

                                                    return (
                                                        <tr key={index}>
                                                            <td>{(index === 0 || date !== previousDate) ? date : null}</td>
                                                            <td>{time}</td>
                                                            <td className={predictionClass}>{doc.AQI_prediction}</td>
                                                            <td className={predictionClass}>{doc.AQI_dis}</td>
                                                        </tr>
                                                    );
                                                })}
                                            </tbody>
                                        </table>
                                    </div>
                                )
                            }
                        </div>
                    </div>
                )}
            </div>
            <div ref={mapContainer} className="map-container" />
        </div>
    );
}

export default App;

