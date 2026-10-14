// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/// @notice Only public crop labels and fingerprints belong on chain.
contract UzoAgroTrace {
    struct Batch { address creator; string crop; bytes32 dataHash; uint256 createdAt; bool exists; }
    struct Checkpoint { string stage; string location; bytes32 dataHash; uint256 timestamp; address recordedBy; }
    mapping(bytes32 => Batch) private batches;
    mapping(bytes32 => Checkpoint[]) private checkpoints;
    mapping(bytes32 => mapping(address => bool)) public operators;
    event BatchCreated(bytes32 indexed id, address indexed creator, bytes32 dataHash);
    event CheckpointAdded(bytes32 indexed id, uint256 index, bytes32 dataHash);

    function createBatch(bytes32 id, string calldata crop, bytes32 dataHash) external {
        require(!batches[id].exists, "Batch exists");
        require(bytes(crop).length > 0 && bytes(crop).length <= 64 && dataHash != bytes32(0), "Invalid data");
        batches[id] = Batch(msg.sender, crop, dataHash, block.timestamp, true);
        emit BatchCreated(id, msg.sender, dataHash);
    }
    function setOperator(bytes32 id, address operator, bool allowed) external {
        require(batches[id].exists && batches[id].creator == msg.sender, "Creator only");
        operators[id][operator] = allowed;
    }
    function addCheckpoint(bytes32 id, string calldata stage, string calldata location, bytes32 dataHash) external {
        require(batches[id].exists, "Unknown batch");
        require(batches[id].creator == msg.sender || operators[id][msg.sender], "Not authorized");
        require(bytes(stage).length > 0 && bytes(stage).length <= 64 && bytes(location).length <= 120 && dataHash != bytes32(0), "Invalid data");
        checkpoints[id].push(Checkpoint(stage, location, dataHash, block.timestamp, msg.sender));
        emit CheckpointAdded(id, checkpoints[id].length - 1, dataHash);
    }
    function getBatch(bytes32 id) external view returns (Batch memory) { return batches[id]; }
    function getBatchCheckpoints(bytes32 id) external view returns (Checkpoint[] memory) { return checkpoints[id]; }
}
