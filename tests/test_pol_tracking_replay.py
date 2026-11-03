import asyncio
import os
from types import SimpleNamespace

from quantnet_agent.hal.driver.dummy import (
    DEFAULT_DUMMY_RESULTS_FILE,
    POL_TRACKING_CHANNELS,
    DummyExpFramework,
    DummyPolarimeter,
    _load_dummy_results,
)


class DummyMsgClient:
    def __init__(self):
        self.published = []

    async def publish(self, topic, payload):
        self.published.append((topic, payload))


def test_dummy_results_json_loads_pol_tracking_samples():
    assert os.path.isfile(DEFAULT_DUMMY_RESULTS_FILE)
    data = _load_dummy_results()
    pol_data = data["pol_tracking"]
    samples = pol_data["samples"]

    assert len(samples) == 26
    for sample in samples:
        assert tuple(sample.keys()) == POL_TRACKING_CHANNELS
        assert all(val >= 0.0 for val in sample.values())

    assert samples[0]["Alice_H2"] == 0.001
    assert max(s["Alice_H2"] for s in samples) == 0.2306
    assert max(samples[-1].values()) < 0.1


def test_dummy_polarimeter_measure_publishes_to_experiment_data():
    node_def = {"systemSettings": {"ID": "bsm1_2", "type": "BSMNode"}}
    polarimeter = DummyPolarimeter({"sample_interval": 0.0}, node_def)
    msgclient = DummyMsgClient()
    polarimeter._msgclient = msgclient

    rc = asyncio.run(polarimeter.measure(exp_id="cal-exp-123"))
    assert rc == 0
    assert len(msgclient.published) == 26

    first_topic, first_payload = msgclient.published[0]
    assert first_topic == "experiment_data"
    assert first_payload["cid"] == "bsm1_2"
    assert first_payload["expid"] == "cal-exp-123"
    assert isinstance(first_payload["ts"], float)
    assert tuple(first_payload["data"]["pol_tracking"].keys()) == POL_TRACKING_CHANNELS

    summary = polarimeter.last_calibration_result
    assert summary is not None
    assert summary["status"] == "done"
    assert summary["samples_replayed"] == 26
    assert summary["initial_max_error"] == 0.2306
    assert summary["final_max_error"] < 0.1
    assert summary["stabilized"] is True
    assert "pol_tracking_series" in summary
    assert len(summary["pol_tracking_series"]) == 26
    assert summary["pol_tracking_series"][0]["pol_tracking"]["Alice_H2"] == 0.001
    assert max(item["pol_tracking"]["Alice_H2"] for item in summary["pol_tracking_series"]) == 0.2306
    assert summary["pol_tracking_series"][-1]["pol_tracking"] == summary["final_pol_tracking"]


def test_dummy_exp_framework_calibration_sequence_and_receive():
    async def _run():
        node_def = {"systemSettings": {"ID": "bsm1_2", "type": "BSMNode"}}
        polarimeter = DummyPolarimeter({"sample_interval": 0.01}, node_def)
        exp_fw = DummyExpFramework({"sample_interval": 0.01}, node_def)
        msgclient = DummyMsgClient()
        hal = SimpleNamespace(devs={"polarimeter": polarimeter, "exp_framework": exp_fw})

        polarimeter._msgclient = msgclient
        polarimeter._hal = hal
        exp_fw._msgclient = msgclient
        exp_fw._hal = hal

        exp_id = "cal-seq-001"
        await exp_fw.submit(exp_id, "calibration.dstInit", None, {"exp_type": "calibration"})

        # Start receive(exp_id) concurrently right after dstInit (mimicking RequestTranslator.getResult)
        recv_task = asyncio.create_task(exp_fw.receive(exp_id))
        await asyncio.sleep(0.02)
        assert not recv_task.done()

        # Run calibration.calibration and calibration.cleanUp concurrently (cleanUp firing early)
        cal_task = asyncio.create_task(
            exp_fw.submit(exp_id, "calibration.calibration", None, {"exp_type": "calibration"})
        )
        await asyncio.sleep(0.02)
        await exp_fw.submit(exp_id, "calibration.cleanUp", None, {"exp_type": "calibration"})

        # receive() must wait until calibration.calibration finishes all 26 samples
        result = await recv_task
        await cal_task

        assert len(msgclient.published) == 26
        rec = exp_fw._experiments[exp_id]
        assert str(rec.id) == exp_id
        assert str(rec.phase) == "done"
        assert result["status"] == "done"
        assert result["exp_type"] == "calibration"
        assert result["role"] == "dst"
        assert result["samples_replayed"] == 26
        assert result["stabilized"] is True
        assert len(result["pol_tracking_series"]) == 26
        assert "calibration.dstInit" in result["steps"]
        assert "calibration.calibration" in result["steps"]
        assert "calibration.cleanUp" in result["steps"]

    asyncio.run(_run())


def test_dummy_exp_framework_receive_by_experiment_type():
    async def _run():
        qnode_def = {"systemSettings": {"ID": "LBNL-Q", "type": "QNode"}}
        bsm_def = {"systemSettings": {"ID": "LBNL-BSM", "type": "BSMNode"}}
        q_fw = DummyExpFramework({"sample_interval": 0.0}, qnode_def)
        bsm_fw = DummyExpFramework({"sample_interval": 0.0}, bsm_def)

        # 1. Local calibration (Cal397Freq / Cal397Att)
        await q_fw.submit(
            "local-cal-001",
            "experiments/Calibrations/cal_397_frequency.py",
            "Cal397Freq",
            {"exp_type": "local_calibration", "freq_range": [100, 200]},
        )
        local_res = await q_fw.receive("local-cal-001")
        assert local_res["status"] == "done"
        assert local_res["dummy status"] == "done"
        assert local_res["exp_type"] == "local_calibration"
        assert "current_scan.plots.x" in local_res["results"]

        # 2. SPG experiment
        await q_fw.submit(
            "spg-001",
            "experiments/single_photon_generation.py",
            "SinglePhotonGeneration",
            {"exp_type": "spg", "iterations": 500},
        )
        spg_res = await q_fw.receive("spg-001")
        assert spg_res["status"] == "done"
        assert spg_res["exp_type"] == "spg"
        assert spg_res["node"] == "LBNL-Q"
        assert spg_res["photon_attempts"] == 10000
        assert spg_res["photons_detected"] == 842
        assert spg_res["collection_efficiency"] == 0.0842
        assert spg_res["parameters"] == {"iterations": 500}
        assert spg_res["steps"] == ["experiments/single_photon_generation.py"]

        # 3. BSM experiment
        await bsm_fw.submit(
            "bsm-001",
            "BSM",
            "BSM",
            {"exp_type": "bsm", "bsm_Counts": [12, 14]},
        )
        bsm_res = await bsm_fw.receive("bsm-001")
        assert bsm_res["status"] == "done"
        assert bsm_res["exp_type"] == "bsm"
        assert bsm_res["node"] == "LBNL-BSM"
        assert bsm_res["psi_plus_counts"] == 128
        assert bsm_res["psi_minus_counts"] == 134
        assert bsm_res["total_coincidences"] == 262
        assert bsm_res["visibility"] == 0.942
        assert bsm_res["parameters"] == {"bsm_Counts": [12, 14]}

        # 4. EGP experiment (QNode DdsOutput and BSMNode BSMnodeBlackQuantum)
        await q_fw.submit(
            "egp-001",
            "experiments/dds_output.py",
            "DdsOutput",
            {"exp_type": "egp", "pairs": 25},
        )
        egp_q_res = await q_fw.receive("egp-001")
        assert egp_q_res["status"] == "done"
        assert egp_q_res["exp_type"] == "egp"
        assert egp_q_res["node"] == "LBNL-Q"
        assert egp_q_res["bell_pairs_generated"] == 10
        assert egp_q_res["fidelity"] == 0.935
        assert egp_q_res["parameters"] == {"pairs": 25}

        await bsm_fw.submit(
            "egp-001",
            "BSMnodeBlackQuantum.py",
            "BSMnodeBlackQuantum",
            {"exp_type": "egp"},
        )
        egp_bsm_res = await bsm_fw.receive("egp-001")
        assert egp_bsm_res["status"] == "done"
        assert egp_bsm_res["exp_type"] == "egp"
        assert egp_bsm_res["node"] == "LBNL-BSM"
        assert egp_bsm_res["bell_pairs_generated"] == 10

        # 5. DQC experiment (mix of prefill, gate blocks, and injected EGP DdsOutput)
        await q_fw.submit("dqc-001", "entanglement_prefill", "entanglement_prefill", {"exp_type": "dqc"})
        await q_fw.submit("dqc-001", "Block_0_H", "Block_0_H", {"exp_type": "dqc"})
        await q_fw.submit("dqc-001", "experiments/dds_output.py", "DdsOutput", {"exp_type": "dqc"})
        await q_fw.submit("dqc-001", "Block_1_CX", "Block_1_CX", {"exp_type": "dqc"})
        dqc_res = await q_fw.receive("dqc-001")
        assert dqc_res["status"] == "done"
        assert dqc_res["exp_type"] == "dqc"
        assert dqc_res["node"] == "LBNL-Q"
        assert dqc_res["prefill_blocks_executed"] == 1
        assert dqc_res["gate_blocks_executed"] == 2
        assert dqc_res["entanglement_blocks_executed"] == 1
        assert dqc_res["gate_fidelity"] == 0.992

        # 5b. DQC on pure-EGP BSM node with explicit exp_type="dqc"
        await bsm_fw.submit(
            "dqc-001",
            "experiments/dds_output.py",
            "DdsOutput",
            {"exp_type": "dqc"},
        )
        dqc_bsm_res = await bsm_fw.receive("dqc-001")
        assert dqc_bsm_res["status"] == "done"
        assert dqc_bsm_res["exp_type"] == "dqc"
        assert dqc_bsm_res["node"] == "LBNL-BSM"
        assert dqc_bsm_res["entanglement_blocks_executed"] == 1
        assert dqc_bsm_res["gate_blocks_executed"] == 0

        # 6. Link calibration source role (srcInit + generation + cleanUp)
        await q_fw.submit(
            "cal-src-001", "calibration.srcInit", "CalibrationSrcInit", {"exp_type": "calibration", "power": 1.0}
        )
        await q_fw.submit("cal-src-001", "calibration.generation", "CalibrationGeneration", {"exp_type": "calibration"})
        await q_fw.submit("cal-src-001", "calibration.cleanUp", "CalibrationCleanup", {"exp_type": "calibration"})
        cal_src_res = await q_fw.receive("cal-src-001")
        assert cal_src_res["status"] == "done"
        assert cal_src_res["exp_type"] == "calibration"
        assert cal_src_res["role"] == "src"
        assert cal_src_res["parameters"] == {"power": 1.0}
        assert "pol_tracking_series" not in cal_src_res

        # 7. Generic experiment fallback
        await q_fw.submit("custom-001", "experiments/custom_seq.py", "CustomSeq", {"foo": "bar"})
        gen_res = await q_fw.receive("custom-001")
        assert gen_res["status"] == "done"
        assert gen_res["exp_type"] == "generic"
        assert gen_res["node"] == "LBNL-Q"
        assert gen_res["parameters"] == {"foo": "bar"}

    asyncio.run(_run())
