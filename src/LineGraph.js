import React from 'react';
import Dygraph from 'dygraphs';
import { useState, useEffect } from 'react';
import './LineGraph.scss'

export default function Graph({ dataBuffer }) {
    const [graph, setGraph] = useState();

    useEffect(() => {
        if (dataBuffer.length === 0) {
            graph && graph.destroy()
            setGraph();
            return
        }

        if (!graph) {
            let graphInstance = new Dygraph(document.getElementById("vis"), dataBuffer, {
                labels: ["hour", "susceptible", "infected", "quarantined", "recovered", "deceased"],
                legend: 'always',
                animatedZooms: true,
                title: 'Time Series Graph',
                ylabel: 'Number of Agents',
                xlabel: 'Hours'
            });

            setGraph(graphInstance);
        }
        else {
            graph.updateOptions({ 'file': dataBuffer });
        }

    }, [graph, dataBuffer])

    return <div id="vis" data-testid="visualization" style={{ width: "85%", height: "600px" }}></div>;

}