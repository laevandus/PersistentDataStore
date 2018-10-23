import UIKit

final class PersistentDataStore {
    
    // MARK: Creating the Data Store
    
    let name: String
    private let dataStoreURL: URL
    private let queue: DispatchQueue
    
    init(name: String) throws {
        self.name = name
        queue = DispatchQueue(label: "com.augmentedcode.persistentdatastore", qos: .userInitiated, attributes: .concurrent, autoreleaseFrequency: .workItem)
        let documentsURL = try FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
        dataStoreURL = documentsURL.appendingPathComponent(name, isDirectory: true)
        try FileManager.default.createDirectory(at: dataStoreURL, withIntermediateDirectories: true, attributes: nil)
    }
    
    private func url(forIdentifier identifier: Identifier) -> URL {
        return dataStoreURL.appendingPathComponent(identifier.trimmingCharacters(in: .whitespacesAndNewlines), isDirectory: false)
    }
    
    // MARK: Loading Data
    
    func loadData<T>(forIdentifier identifier: Identifier, dataTransformer: @escaping (Data) -> (T?), completionHandler block: @escaping (T?) -> ()) {
        queue.async {
            let url = self.url(forIdentifier: identifier)
            guard FileManager.default.fileExists(atPath: url.path) else {
                DispatchQueue.main.async {
                    block(nil)
                }
                return
            }
            do {
                let data = try Data(contentsOf: url, options: .mappedIfSafe)
                let object = dataTransformer(data)
                DispatchQueue.main.async {
                    block(object)
                }
            }
            catch {
                print("Failed reading data at URL \(url).")
                DispatchQueue.main.async {
                    block(nil)
                }
            }
        }
    }
    
    // MARK: Storing Data
    
    typealias Identifier = String
    
    enum Result {
        case failed(Error)
        case noData
        case success(Identifier)
    }
    
    func storeData(_ dataProvider: @escaping () -> (Data?), identifier: Identifier = UUID().uuidString, completionHandler block: @escaping (Result) -> ()) {
        queue.async(flags: .barrier) {
            let url = self.url(forIdentifier: identifier)
            guard let data = dataProvider(), !data.isEmpty else {
                DispatchQueue.main.async {
                    block(.noData)
                }
                return
            }
            do {
                try data.write(to: url, options: .atomic)
                DispatchQueue.main.async {
                    block(.success(identifier))
                }
            }
            catch {
                DispatchQueue.main.async {
                    block(.failed(error))
                }
            }
        }
    }
    
    // MARK: Removing Data
    
    func removeData(forIdentifier identifier: Identifier) {
        queue.async(flags: .barrier) {
            let url = self.url(forIdentifier: identifier)
            guard FileManager.default.fileExists(atPath: url.path) else { return }
            do {
                try FileManager.default.removeItem(at: url)
            }
            catch {
                print("Failed removing file at URL \(url) with error \(error).")
            }
        }
    }
    
    func removeAll() {
        queue.async(flags: .barrier) {
            do {
                let urls = try FileManager.default.contentsOfDirectory(at: self.dataStoreURL, includingPropertiesForKeys: nil, options: [])
                try urls.forEach({ try FileManager.default.removeItem(at: $0) })
            }
            catch {
                print("Failed removing all files with error \(error).")
            }
        }
    }
}

extension PersistentDataStore {
    func loadImage(forIdentifier identifier: Identifier, completionHandler block: @escaping (UIImage?) -> (Void)) {
        loadData(forIdentifier: identifier, dataTransformer: { UIImage(data: $0) }, completionHandler: block)
    }

    func storeImage(_ image: UIImage, identifier: Identifier = UUID().uuidString, completionHandler handler: @escaping (Result) -> ()) {
        storeData({ image.jpegData(compressionQuality: 1.0) }, identifier: identifier, completionHandler: handler)
    }
}

guard let image = UIImage(named: "Chroma.jpg") else { fatalError() }
do {
    let persistentStore = try PersistentDataStore(name: "Example")
    
    // Using designated write function.
    persistentStore.storeData({ () -> (Data?) in
        return image.jpegData(compressionQuality: 1.0)
    }) { (result) in
        switch result {
        case .success(let identifier):
            print("Stored data successfully with identifier \(identifier).")
        case .noData:
            print("No data to store.")
        case .failed(let error):
            print("Failed storing data with error \(error)")
        }
    }
    
    // Using designated write function and custom identifier (rewrites data on disk if there is already data stored with that identifier).
    persistentStore.storeData({ () -> (Data?) in
        return image.jpegData(compressionQuality: 1.0)
    }, identifier: "my_identifier") { (result) in
        print(result)
    }
    
    // Using convenience method.
    persistentStore.storeImage(image) { (result) in
        print(result)
    }
    
    // Loading data.
    persistentStore.loadData(forIdentifier: "my_identifier", dataTransformer: { UIImage(data: $0) }) { (image) in
        guard let image = image else {
            print("Failed loading image.")
            return
        }
        print(image)
    }
    
    // Loading data using convenience method.
    persistentStore.loadImage(forIdentifier: "my_identifier") { (image) -> (Void) in
        guard let image = image else {
            print("Failed loading image.")
            return
        }
        print(image)
    }
    
    // Removing data.
    persistentStore.removeData(forIdentifier: "my_identifier")
    persistentStore.removeAll()
    
    // Store again.
    persistentStore.storeImage(image, identifier: "my_identifier") { (result) in
        print(result)
    }
}
catch {
    print(error)
}
