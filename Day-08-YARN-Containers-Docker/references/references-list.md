# Day 8: References & Deep Reads

Here are the official documentation resources, whitepapers, and engineering blog posts referenced in this module:

## 🌐 Official Specifications & Guides

*   **[Apache Hadoop YARN Docker Containers Guide](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/DockerContainers.html)**: The primary documentation on configuring NodeManager to run Docker containers.
*   **[Hadoop LinuxContainerExecutor Setup](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/SecureContainer.html)**: Step-by-step instructions on setting up `LinuxContainerExecutor` securely with user mappings.
*   **[Docker Runtime Configuration Guide](https://docs.docker.com/engine/reference/commandline/dockerd/)**: Official details on Docker daemon configuration, socket security, and network settings.

## 🐧 OS Kernel & Isolation References

*   **[Linux CGroups v1 Documentation](https://www.kernel.org/doc/Documentation/cgroup-v1/)**: Detailed operational mechanics of control groups for CPU, memory, and devices.
*   **[Linux Namespaces Guide](https://man7.org/linux/man-pages/man7/namespaces.7.html)**: Reference on process namespaces (PID, IPC, Net, User, Mount) providing lightweight virtualization.

## 📰 Engineering Blogs & Case Studies

*   **[Hadoop + Docker at Scale (Hortonworks Engineering Blog)](https://blog.cloudera.com/)**: Practical case studies on operational benefits of containerized workloads in large multi-tenant clusters.
*   **[Running Spark on YARN with Docker](https://spark.apache.org/docs/latest/running-on-yarn.html)**: Best practices for packing Spark dependencies inside Docker containers and running on YARN NodeManagers.
